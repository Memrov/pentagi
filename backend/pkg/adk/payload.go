package adk

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

// PayloadStore holds oversized tool output outside the activity result, so a
// large scan does not bloat workflow history. The model sees a preview and the
// URI; payload_read resolves that URI back through Get.
type PayloadStore interface {
	Put(ctx context.Context, key string, data []byte) (uri string, err error)
	Get(ctx context.Context, uri string) (data []byte, err error)
}

// MemoryPayloadStore is the in-process fallback when no bucket is configured;
// its blobs die with the worker, so its URIs are only meaningful while it
// lives. Entries are bounded so an output-heavy run cannot grow memory
// unboundedly — oldest keys are evicted FIFO.
const maxMemoryPayloads = 32

type MemoryPayloadStore struct {
	mu    sync.Mutex
	blobs map[string][]byte
	order []string
}

func NewMemoryPayloadStore() *MemoryPayloadStore {
	return &MemoryPayloadStore{blobs: map[string][]byte{}}
}

func (s *MemoryPayloadStore) Put(_ context.Context, key string, data []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.blobs == nil {
		s.blobs = map[string][]byte{}
	}
	if _, exists := s.blobs[key]; !exists {
		s.order = append(s.order, key)
		for len(s.order) > maxMemoryPayloads {
			delete(s.blobs, s.order[0])
			s.order = s.order[1:]
		}
	}
	s.blobs[key] = slices.Clone(data)
	return "mem://" + key, nil
}

func (s *MemoryPayloadStore) Get(_ context.Context, uri string) ([]byte, error) {
	key, ok := strings.CutPrefix(uri, "mem://")
	if !ok {
		return nil, fmt.Errorf("not a mem payload uri: %q", uri)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.blobs[key]
	if !ok {
		return nil, fmt.Errorf("payload %q not found", uri)
	}
	return slices.Clone(data), nil
}

// s3API is the object API the store makes, narrow enough to fake in tests.
type s3API interface {
	PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// S3PayloadStore writes blobs under "<prefix>/<date>/<uuid>.log" in one bucket;
// a GCS-compatible endpoint is reached by pointing the s3 client at it.
type S3PayloadStore struct {
	client s3API
	bucket string
	prefix string
}

func NewS3PayloadStore(ctx context.Context, bucket, prefix string) (*S3PayloadStore, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load aws config: %w", err)
	}
	return &S3PayloadStore{client: s3.NewFromConfig(cfg), bucket: bucket, prefix: prefix}, nil
}

func (s *S3PayloadStore) Put(ctx context.Context, key string, data []byte) (string, error) {
	if key == "" {
		key = fmt.Sprintf("%s/%s.log", time.Now().UTC().Format("2006-01-02"), uuid.NewString())
	}
	full := s.prefix + "/" + key
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(full),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return "", fmt.Errorf("failed to put payload to s3://%s/%s: %w", s.bucket, full, err)
	}
	return fmt.Sprintf("s3://%s/%s", s.bucket, full), nil
}

// Get resolves an s3:// URI the store issued back to its object; a URI in a
// different bucket is refused rather than fetched — the reader must not reach
// into buckets this worker was never configured to serve.
func (s *S3PayloadStore) Get(ctx context.Context, uri string) ([]byte, error) {
	bucket, key, found := strings.Cut(strings.TrimPrefix(uri, "s3://"), "/")
	if !found || bucket != s.bucket {
		return nil, fmt.Errorf("payload uri %q is outside bucket %q", uri, s.bucket)
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get payload from %s: %w", uri, err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read payload from %s: %w", uri, err)
	}
	return data, nil
}
