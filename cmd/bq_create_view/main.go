package main

import (
	"context"
	"flag"
	"fmt"
	"time"

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

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})
}

func parseTableID(id string) *bigquery.Table {
	fields := strings.Split(id, ".")
	if len(fields) != 3 {
		log.Errorf("Failed to parse table id: %q - found %d fields", id, len(fields))
		return &bigquery.Table{}
	}
	return &bigquery.Table{
		ProjectID: fields[0],
		DatasetID: fields[1],
		TableID:   fields[2]}
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

// tableInterface wraps methods provied by the bigquery.Table for unit tests.
type tableInterface interface {
	Metadata(ctx context.Context) (*bigquery.TableMetadata, error)
	Create(ctx context.Context, tm *bigquery.TableMetadata) error
	Update(ctx context.Context, tm bigquery.TableMetadataToUpdate, etag string) (*bigquery.TableMetadata, error)
}

func syncView(ctx context.Context, tb tableInterface, view *bigquery.Table, sql, description string) error {
	md, err := tb.Metadata(ctx)
	if err != nil {
		apiErr, ok := err.(*googleapi.Error)
		if !ok {
			// This is not a googleapi.Error, so treat it as fatal.
			return err
		}
		// We can only handle 404 errors caused by the view not existing.
		if apiErr.Code != 404 {
			return err
		}
		log.Info("Creating view: ", id(view))
		return tb.Create(ctx, &bigquery.TableMetadata{
			ViewQuery: sql, Description: description})
	}
	log.Info("Updating view: ", id(view))
	_, err = tb.Update(ctx, bigquery.TableMetadataToUpdate{
		ViewQuery: sql, Description: description}, md.ETag)
	return err
}

type datasetInterface interface {
	Metadata(ctx context.Context) (*bigquery.DatasetMetadata, error)
	Update(ctx context.Context, tm bigquery.DatasetMetadataToUpdate, etag string) (*bigquery.DatasetMetadata, error)
}

func syncDatasetAccess(ctx context.Context, ds datasetInterface, view, target *bigquery.Table) error {
	md, err := ds.Metadata(ctx)
	if err != nil {
		return err
	}

	log.Info("Checking whether view can access target table")
	if canAccess(view, md.Access) {
		log.Info("Confirmed: view access is enabled")
		return nil
	}

	log.Infof("Adding access: %s can access %s", id(view), id(target))
	// Note: it's possible for md.Access to include AccessEntries for views that no
	// longer exist. If that's the case, then the Update below will fail.
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
	if err != nil {
		return err
	}
	if !canAccess(view, md2.Access) {
		return fmt.Errorf("Failed to update access to %q", id(target))
	}
	return nil
}

func main() {
	flag.Parse()
	log.SetLevel(log.Level(*logLevel))

	if *viewSource == "" || *accessTarget == "" {
		log.Fatal("Flags --create-view and --to-access must be specified.")
	}

	// Parsing flags.
	view := parseTableID(*viewSource)
	target := parseTableID(*accessTarget)
	sql := fmt.Sprintf("#standardSQL\nSELECT * FROM `%s`", id(target))

	// Create a context that expires after 1 min.
	ctx, cancelCtx := context.WithTimeout(context.Background(), time.Minute)
	defer cancelCtx()

	log.Info("Creating bigquery clients")
	targetClient, err := bigquery.NewClient(ctx, target.ProjectID)
	rtx.Must(err, "Failed to create bigquery.Client")
	viewClient, err := bigquery.NewClient(ctx, view.ProjectID)
	rtx.Must(err, "Failed to create bigquery.Client")

	// Create or Update view query and description.
	log.Info("Reading view metadata: ", id(view))
	tb := viewClient.Dataset(view.DatasetID).Table(view.TableID)
	err = syncView(ctx, tb, view, sql, *description)
	rtx.Must(err, "Failed to sync view %q", id(view))

	// Verify or Add view access to target table.
	log.Info("Reading target dataset metadata: ", id(target))
	ds := targetClient.Dataset(target.DatasetID)
	err = syncDatasetAccess(ctx, ds, view, target)
	rtx.Must(err, "Failed to grant access to ds: %q", id(target))

	log.Info("Success!")
}
