package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"regexp"
	"text/template"
	"time"

	log "github.com/sirupsen/logrus"

	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"
	"google.golang.org/api/googleapi"
)

var (
	srcProject   = flag.String("src-project", "", "Project id of source data, used to evaluate template")
	viewSource   = flag.String("create-view", "", "Full name of BQ view id: <project>.<dataset>.<view>")
	description  = flag.String("description", "", "Description for view")
	editor       = flag.String("editor", "", "User name that should have edit access to the view dataset.")
	logLevel     = flag.Int("log.level", 4, "Log level")
	viewTemplate = flagx.FileBytes{}
)

func init() {
	flag.Var(&viewTemplate, "template", "File name of view query template. Use at most one '%s' to refer to target project.")

	log.SetFormatter(&log.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})
}

// findTables takes the complete text of an SQL query and extracts and returns
// every BQ SQL table pattern.
func findTables(sql string) []string {
	found := []string{}
	segment := "[{}A-Za-z0-9._-]+"
	r := regexp.MustCompile("`(" + segment + "\\." + segment + "\\." + segment + ")`")
	f := r.FindAllStringSubmatch(sql, -1)
	for _, results := range f {
		if len(results) == 2 {
			found = append(found, results[1])
		}
	}
	return found
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

func userCanAccess(user string, access []*bigquery.AccessEntry) bool {
	for i := range access {
		if access[i].EntityType == bigquery.UserEmailEntity {
			if access[i].Entity == user {
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
	Create(ctx context.Context, tm *bigquery.DatasetMetadata) error
}

func syncDataset(ctx context.Context, ds datasetInterface, user string) error {
	md, err := ds.Metadata(ctx)
	if err != nil {
		switch apiErr := err.(type) {
		case *googleapi.Error:
			// We can only handle 404 errors caused by the view not existing.
			if apiErr.Code != 404 {
				return err
			}
		default:
			// This is not a googleapi.Error, so treat it as fatal.
			return err
		}
		if user == "" {
			return fmt.Errorf("User must not be empty")
		}
		log.Info("Creating dataset")
		err = ds.Create(ctx, &bigquery.DatasetMetadata{
			Access: []*bigquery.AccessEntry{
				// Default access entries.
				{Role: bigquery.OwnerRole, EntityType: bigquery.SpecialGroupEntity, Entity: "projectOwners"},
				{Role: bigquery.WriterRole, EntityType: bigquery.SpecialGroupEntity, Entity: "projectWriters"},
				{Role: bigquery.ReaderRole, EntityType: bigquery.SpecialGroupEntity, Entity: "projectReaders"},
				// Access entry for service accounts or individual users beyond the default.
				{Role: bigquery.WriterRole, EntityType: bigquery.UserEmailEntity, Entity: user},
			},
		})
		return err
	}

	// If user is already present in the access entry list, then we're done.
	if user == "" || userCanAccess(user, md.Access) {
		return nil
	}

	// Add user to the AccessEntry.
	//
	// Note: in order to create views in a dataset, a service-account actor must
	// have "writer" access in the AccessEntry for the dataset. This is true even
	// if the service-account previously created the dataset.
	//
	// So, if the process is authenticated as a service-account and that user is
	// trying to add itself, then it may not have permission to do so (even if it
	// has BigQuery Editor role). In this case, the Update will fail and someone
	// with greater privileges must update the AccessEntry on the user's behalf.
	acl := append(md.Access, &bigquery.AccessEntry{
		Role: bigquery.WriterRole, EntityType: bigquery.UserEmailEntity, Entity: user})
	_, err = ds.Update(ctx, bigquery.DatasetMetadataToUpdate{Access: acl}, md.ETag)
	return err
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

	// Access entries to the same project and dataset are unnecessary (and an error).
	if view.ProjectID == target.ProjectID && view.DatasetID == target.DatasetID {
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

	if *viewSource == "" || len(viewTemplate) == 0 {
		flag.Usage()
		log.Fatal("--create-view, --to-access, and --template flags are required.")
	}

	// Parsing flags.
	view := parseTableID(*viewSource)
	src := &bigquery.Table{
		ProjectID: *srcProject,
	}

	// Evaluate viewTemplate.
	var viewContent bytes.Buffer
	tmpl := template.Must(template.New("template").Parse(viewTemplate.String()))
	rtx.Must(tmpl.Execute(&viewContent, src), "Failed to execute view template: %q", viewTemplate)
	sql := viewContent.String()
	tables := findTables(sql)

	// Create a context that expires after 1 min.
	ctx, cancelCtx := context.WithTimeout(context.Background(), time.Minute)
	defer cancelCtx()

	log.Info("Creating bigquery clients")
	viewClient, err := bigquery.NewClient(ctx, view.ProjectID)
	rtx.Must(err, "Failed to create bigquery.Client")

	// Create or Update view dataset.
	log.Info("Syncing view dataset: ", id(view))
	viewDs := viewClient.Dataset(view.DatasetID)
	err = syncDataset(ctx, viewDs, *editor)
	rtx.Must(err, "Failed to sync dataset: %q", viewDs.DatasetID)

	// Verify or Add view access to target table.
	for _, table := range tables {
		target := parseTableID(table)
		log.Info("Reading target dataset metadata: ", id(target))
		targetClient, err := bigquery.NewClient(ctx, target.ProjectID)
		rtx.Must(err, "Failed to create bigquery.Client")
		ds := targetClient.Dataset(target.DatasetID)
		err = syncDatasetAccess(ctx, ds, view, target)
		rtx.Must(err, "Failed to grant access to ds: %q", id(target))
	}

	// Create or Update view query and description.
	log.Info("Reading view metadata: ", id(view))
	tb := viewDs.Table(view.TableID)
	err = syncView(ctx, tb, view, sql, *description)
	rtx.Must(err, "Failed to sync view %q", id(view))

	log.Info("Success!")
}
