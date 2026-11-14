package adk

import (
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

	data, ok := s.Get("k/1.log")
	require.True(t, ok)
	assert.Equal(t, []byte("payload-bytes"), data)
}

type fakeS3API struct {
	bucket, key string
	body        []byte
	err         error
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
