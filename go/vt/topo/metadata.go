/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topo

import (
	"context"
	"path"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/vt/topo/events"
)

// UpsertMetadata sets the key/value in the metadata if it doesn't exist, otherwise it updates the content
func (ts *Server) UpsertMetadata(ctx context.Context, keyspace string, key string, val string) error {
	keyPath := path.Join(MetadataPath, keyspace, key)
	_, version, err := ts.globalCell.Get(ctx, keyPath)
	if err != nil && IsErrType(err, NoNode) {
		return create(ctx, ts, keyspace, keyPath, val)
	}

	return update(ctx, ts, keyspace, keyPath, val, version)
}

// GetMetadata retrieves the metadata value for the given key
func (ts *Server) GetMetadata(ctx context.Context, keyspace string, key string) (string, error) {
	keyPath := path.Join(MetadataPath, keyspace, key)
	contents, _, err := ts.globalCell.Get(ctx, keyPath)
	if err != nil {
		return "", err
	}

	return string(contents), nil
}

func create(ctx context.Context, ts *Server, keyspace string, path string, val string) error {
	if _, err := ts.globalCell.Create(ctx, path, []byte(val)); err != nil {
		return err
	}

	dispatchEvent(keyspace, path, "created")
	return nil
}

func update(ctx context.Context, ts *Server, keyspace string, path string, val string, version Version) error {
	if _, err := ts.globalCell.Update(ctx, path, []byte(val), version); err != nil {
		return err
	}

	dispatchEvent(keyspace, path, "updated")
	return nil
}

func dispatchEvent(keyspace string, key string, status string) {
	event.Dispatch(&events.MetadataChange{
		KeyspaceName: keyspace,
		Key:          key,
		Status:       status,
	})
}
