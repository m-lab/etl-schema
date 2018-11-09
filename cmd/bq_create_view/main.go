package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/stephen-soltesz/pretty"

	log "github.com/sirupsen/logrus"

	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/go/rtx"
	"google.golang.org/api/googleapi"
)

var (
	viewSource   = flag.String("create-view", "", "Source view id: <project>.<dataset>.<view>")
	accessTarget = flag.String("to-access", "", "Target table id accessed by view. Must already exist.")
	description  = flag.String("description", "", "Description for view")
	logLevel     = flag.Int("log.level", 4, "Log level")
)

func parseTableID(id string) (*bigquery.Table, error) {
	fields := strings.Split(id, ".")
	if len(fields) != 3 {
		return nil, fmt.Errorf("Failed to parse: %q - found %d fields", id, len(fields))
	}
	return &bigquery.Table{
		ProjectID: fields[0],
		DatasetID: fields[1],
		TableID:   fields[2]}, nil
}

func canAccess(table *bigquery.Table, access []*bigquery.AccessEntry) bool {
	for i := range access {
		if access[i].View != nil {
			if equalTables(access[i].View, table) {
				return true
			}
		}
	}
	return false
}

func equalTables(a *bigquery.Table, b *bigquery.Table) bool {
	return (a.ProjectID == b.ProjectID && a.DatasetID == b.DatasetID && a.TableID == b.TableID)
}

func id(a *bigquery.Table) string {
	return fmt.Sprintf("%s.%s.%s", a.ProjectID, a.DatasetID, a.TableID)
}

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})
}

func main() {
	flag.Parse()

	log.SetLevel(log.Level(*logLevel))

	view, err := parseTableID(*viewSource)
	rtx.Must(err, "Failed to parse source view id")

	target, err := parseTableID(*accessTarget)
	rtx.Must(err, "Failed to parse target table id")

	log.Info("Creating bigquery clients")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	targetClient, err := bigquery.NewClient(ctx, target.ProjectID)
	rtx.Must(err, "Failed to create bigquery.Client")

	viewClient, err := bigquery.NewClient(ctx, view.ProjectID)
	rtx.Must(err, "Failed to create bigquery.Client")
	tb := viewClient.Dataset(view.DatasetID).Table(view.TableID)

	// Create or Update view query and description.
	meta := &bigquery.TableMetadata{
		ViewQuery:   "#standardSQL\nSELECT * FROM `" + id(target) + "`",
		Description: *description,
	}
	tmd, err := tb.Metadata(ctx)
	if apiErr, ok := err.(*googleapi.Error); ok {
		if apiErr.Code != 404 {
			// Error making request, this is fatal.
			rtx.Must(err, "Failed to get table metadata")
		}
		// Error because view does not exist, so create it.
		log.Info("Creating view: ", id(view))
		err := tb.Create(ctx, meta)
		rtx.Must(err, "Failed to create view")
	} else {
		log.Info("Updating view: ", id(view))
		log.Info("err: ", err)
		pretty.Print(tmd)
		_, err := tb.Update(ctx, bigquery.TableMetadataToUpdate{
			ViewQuery: meta.ViewQuery, Description: meta.Description}, tmd.ETag)
		rtx.Must(err, "Failed to update view")
	}

	// Verify for Add view access to target table.
	log.Info("Reading target table metadata: ", id(target))
	ds := targetClient.Dataset(target.DatasetID)
	md, err := ds.Metadata(ctx)
	rtx.Must(err, "Failed to get dataset metadata")

	log.Info("Checking whether view can access target table")
	if canAccess(view, md.Access) {
		log.Info("Confirmed: view access is enabled")
		return
	}

	log.Infof("Adding access: %q can access %q", id(view), id(target))
	acl := append(md.Access, &bigquery.AccessEntry{
		// Role & Entity fields are not used for view access.
		EntityType: bigquery.ViewEntity,
		View: &bigquery.Table{
			ProjectID: view.ProjectID,
			DatasetID: view.DatasetID,
			TableID:   view.TableID,
		},
	})

	// Apply the updated Access ACL.
	md2, err := ds.Update(ctx, bigquery.DatasetMetadataToUpdate{Access: acl}, md.ETag)
	rtx.Must(err, "Failed to update dataset access")
	if !canAccess(view, md2.Access) {
		log.Fatalf("Failed to update access to %q", id(target))
	}
	log.Info("Success!")
}
