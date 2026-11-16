package adk

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryPayloadStore_PutGet_RoundTrip(t *testing.T) {
	s := NewMemoryPayloadStore()
	uri, err := s.Put(context.Background(), "k/1.log", []byte("payload-bytes"))
	require.NoError(t, err)
	assert.Equal(t, "mem://k/1.log", uri)

	data, err := s.Get(context.Background(), uri)
	require.NoError(t, err)
	assert.Equal(t, []byte("payload-bytes"), data)
}

func TestMemoryPayloadStore_Get_ReportsUnknownAndForeignURIs(t *testing.T) {
	s := NewMemoryPayloadStore()

	_, err := s.Get(context.Background(), "mem://never-put.log")
	assert.ErrorContains(t, err, "not found")

	_, err = s.Get(context.Background(), "s3://other-bucket/key.log")
	assert.ErrorContains(t, err, "not a mem payload uri")
}

type fakeS3API struct {
	bucket, key string
	body        []byte
	err         error
	getErr      error
}

func (f *fakeS3API) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.bucket = *in.Bucket
	f.key = *in.Key
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.body = body
	return &s3.PutObjectOutput{}, f.err
}

func (f *fakeS3API) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	f.bucket = *in.Bucket
	f.key = *in.Key
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(f.body))}, nil
}

func TestS3PayloadStore_Put_KeyLayoutAndURI(t *testing.T) {
	fake := &fakeS3API{}
	s := &S3PayloadStore{client: fake, bucket: "payloads", prefix: "adk-payloads"}

	uri, err := s.Put(context.Background(), "wf-9/act-attempt1.log", []byte("blob"))
	require.NoError(t, err)
	assert.Equal(t, "s3://payloads/adk-payloads/wf-9/act-attempt1.log", uri)
	assert.Equal(t, "payloads", fake.bucket)
	assert.Equal(t, "adk-payloads/wf-9/act-attempt1.log", fake.key)
	assert.Equal(t, []byte("blob"), fake.body)

	uri, err = s.Put(context.Background(), "", []byte("blob"))
	require.NoError(t, err)
	assert.Contains(t, uri, "s3://payloads/adk-payloads/")
}

func TestS3PayloadStore_Get_FetchesStoredKey(t *testing.T) {
	fake := &fakeS3API{body: []byte("stored-blob")}
	s := &S3PayloadStore{client: fake, bucket: "payloads", prefix: "adk-payloads"}

	data, err := s.Get(context.Background(), "s3://payloads/adk-payloads/wf-9/act-attempt1.log")
	require.NoError(t, err)
	assert.Equal(t, []byte("stored-blob"), data)
	assert.Equal(t, "payloads", fake.bucket)
	assert.Equal(t, "adk-payloads/wf-9/act-attempt1.log", fake.key)
}

// A URI outside the configured bucket must never reach the object API — the
// reader only serves what this worker's store could have written.
func TestS3PayloadStore_Get_RefusesForeignBucket(t *testing.T) {
	fake := &fakeS3API{body: []byte("secret")}
	s := &S3PayloadStore{client: fake, bucket: "payloads", prefix: "adk-payloads"}

	_, err := s.Get(context.Background(), "s3://other-bucket/adk-payloads/k.log")
	assert.ErrorContains(t, err, "outside bucket")
	assert.Empty(t, fake.key)

	_, err = s.Get(context.Background(), "mem://not-an-s3-uri")
	assert.ErrorContains(t, err, "outside bucket")
}
