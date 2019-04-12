package main

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

type fakeTable struct {
	md  *bigquery.TableMetadata
	err error
}

func (ft *fakeTable) Metadata(ctx context.Context) (*bigquery.TableMetadata, error) {
	return ft.md, ft.err
}

func (ft *fakeTable) Create(ctx context.Context, tm *bigquery.TableMetadata) error {
	return ft.err
}

func (ft *fakeTable) Update(ctx context.Context, tm bigquery.TableMetadataToUpdate, etag string) (*bigquery.TableMetadata, error) {
	return ft.md, ft.err
}

func Test_syncView(t *testing.T) {
	tests := []struct {
		name    string
		tb      tableInterface
		view    *bigquery.Table
		wantErr bool
	}{
		{
			name: "success",
			tb:   &fakeTable{md: &bigquery.TableMetadata{ETag: "tag123"}},
			view: parseTableID("mlab-testing.sidestream.base"),
		},
		{
			name:    "error-general-error",
			tb:      &fakeTable{err: fmt.Errorf("This is a generic error")},
			wantErr: true,
		},
		{
			name:    "error-googleapi-error-other",
			tb:      &fakeTable{err: &googleapi.Error{Code: 100}},
			wantErr: true,
		},
		{
			name:    "error-googleapi-error-404",
			tb:      &fakeTable{err: &googleapi.Error{Code: 404}},
			view:    parseTableID("mlab-testing.sidestream.base"),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if err := syncView(ctx, tt.tb, tt.view, "SELECT * FROM `foo.bar.baz`", ""); (err != nil) != tt.wantErr {
				t.Errorf("syncView() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

type fakeDataset struct {
	mdErr  error
	upErr  error
	crErr  error
	md     *bigquery.DatasetMetadata
	update *bigquery.DatasetMetadata
}

func (fd *fakeDataset) Metadata(ctx context.Context) (*bigquery.DatasetMetadata, error) {
	return fd.md, fd.mdErr
}
func (fd *fakeDataset) Update(ctx context.Context, tm bigquery.DatasetMetadataToUpdate, etag string) (*bigquery.DatasetMetadata, error) {
	return fd.update, fd.upErr
}
func (fd *fakeDataset) Create(ctx context.Context, tm *bigquery.DatasetMetadata) error {
	return fd.crErr
}

func Test_syncDatasetAccess(t *testing.T) {
	tests := []struct {
		name    string
		ds      datasetInterface
		view    *bigquery.Table
		target  *bigquery.Table
		wantErr bool
	}{
		{
			name: "success-already-found",
			ds: &fakeDataset{
				md: &bigquery.DatasetMetadata{
					Access: []*bigquery.AccessEntry{
						{
							EntityType: bigquery.ViewEntity,
							View:       parseTableID("mlab-testing.sidestream.base"),
						},
					},
				},
			},
			view:   parseTableID("mlab-testing.sidestream.base"),
			target: parseTableID("mlab-testing.base_tables.sidestream"),
		},
		{
			name: "success-update",
			ds: &fakeDataset{
				md: &bigquery.DatasetMetadata{
					Access: []*bigquery.AccessEntry{
						{
							EntityType: bigquery.ViewEntity,
							View:       parseTableID("this.isnot.therightone"),
						},
					},
				},
				update: &bigquery.DatasetMetadata{
					Access: []*bigquery.AccessEntry{
						{
							EntityType: bigquery.ViewEntity,
							View:       parseTableID("mlab-testing.sidestream.base"),
						},
					},
				},
			},
			view:   parseTableID("mlab-testing.sidestream.base"),
			target: parseTableID("mlab-testing.base_tables.sidestream"),
		},
		{
			name:    "error-metadata-fails",
			ds:      &fakeDataset{mdErr: fmt.Errorf("This is an error")},
			wantErr: true,
		},
		{
			name: "error-update-fails",
			ds: &fakeDataset{
				md: &bigquery.DatasetMetadata{
					Access: []*bigquery.AccessEntry{
						{
							EntityType: bigquery.ViewEntity,
							View:       parseTableID("this.isnot.therightone"),
						},
					},
				},
				upErr: fmt.Errorf("This is an error"),
			},
			view:    parseTableID("mlab-testing.sidestream.base"),
			target:  parseTableID("mlab-testing.base_tables.sidestream"),
			wantErr: true,
		},
		{
			name: "error-update-missing-access-entry",
			ds: &fakeDataset{
				md: &bigquery.DatasetMetadata{
					Access: []*bigquery.AccessEntry{
						{
							EntityType: bigquery.ViewEntity,
							View:       parseTableID("this.isnot.therightone"),
						},
					},
				},
				update: &bigquery.DatasetMetadata{
					Access: []*bigquery.AccessEntry{
						{
							EntityType: bigquery.ViewEntity,
							View:       parseTableID("this-is.still-not.the-right-one"),
						},
					},
				},
			},
			view:    parseTableID("mlab-testing.sidestream.base"),
			target:  parseTableID("mlab-testing.base_tables.sidestream"),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if err := syncDatasetAccess(ctx, tt.ds, tt.view, tt.target); (err != nil) != tt.wantErr {
				t.Errorf("syncDatasetAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_parseTableID(t *testing.T) {
	tests := []struct {
		name string
		id   string
		want *bigquery.Table
	}{
		{
			name: "success",
			id:   "mlab-testing.sidestream.base",
			want: parseTableID("mlab-testing.sidestream.base"),
		},
		{
			name: "error-fail",
			id:   "mlab-testing-missing.fields",
			want: &bigquery.Table{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseTableID(tt.id); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseTableID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_syncDataset(t *testing.T) {
	type args struct {
	}
	tests := []struct {
		name    string
		ds      datasetInterface
		user    string
		wantErr bool
	}{
		{
			name: "okay-update",
			ds: &fakeDataset{
				md: &bigquery.DatasetMetadata{
					Access: []*bigquery.AccessEntry{
						{Role: bigquery.OwnerRole, EntityType: bigquery.SpecialGroupEntity, Entity: "projectOwners"},
						{Role: bigquery.WriterRole, EntityType: bigquery.SpecialGroupEntity, Entity: "projectWriters"},
						{Role: bigquery.ReaderRole, EntityType: bigquery.SpecialGroupEntity, Entity: "projectReaders"},
					},
				},
			},
			user: "fake-user",
		},
		{
			name: "okay-create",
			ds: &fakeDataset{
				md:    nil,
				mdErr: &googleapi.Error{Code: 404},
			},
			user: "fake-user",
		},
		{
			name: "okay-user-already-exists",
			ds: &fakeDataset{
				md: &bigquery.DatasetMetadata{
					Access: []*bigquery.AccessEntry{
						{Role: bigquery.WriterRole, EntityType: bigquery.UserEmailEntity, Entity: "fake-user"},
					},
				},
			},
			user: "fake-user",
		},
		{
			name: "error-user-not-specified",
			ds: &fakeDataset{
				md:    nil,
				mdErr: &googleapi.Error{Code: 404},
			},
			user:    "",
			wantErr: true,
		},
		{
			name: "error-access-denied-metadata-error",
			ds: &fakeDataset{
				md:    nil,
				mdErr: &googleapi.Error{Code: 403},
			},
			wantErr: true,
		},
		{
			name: "error-unknown-metadata-error",
			ds: &fakeDataset{
				md:    nil,
				mdErr: fmt.Errorf("This is a fake error that syncDataset can't handle"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		ctx := context.Background()
		t.Run(tt.name, func(t *testing.T) {
			if err := syncDataset(ctx, tt.ds, tt.user); (err != nil) != tt.wantErr {
				t.Errorf("syncDataset() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
