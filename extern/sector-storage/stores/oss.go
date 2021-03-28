package stores

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/xerrors"
	"os"
	"strings"
)

/**
 * Vendor - Region -    Part Size
 * ucloud   sc-test     32MiB
 * qiniu    cn-east-1   63MiB
 * ceph     us-west-2   8MiB
 * minio    us-west-2   8MiB
 */

type OSSInfo struct {
	URL            string
	AccessKey      string
	SecretKey      string
	BucketName     string
	Prefix         string
	UniqueBucket   bool
	UploadPartSize int64
	CanWrite       bool
	Vendor         string
	Region         string
}

type StorageOSSInfo = OSSInfo

type OSSClient struct {
	s3Client     *s3.S3
	s3Uploader   *s3manager.Uploader
	s3Downloader *s3manager.Downloader
	s3Session    *session.Session
	s3Info       OSSInfo
	proofBucket  string
	dataBucket   string
}

type OSSSector struct {
	name string
}

func (obj *OSSSector) Name() string {
	return obj.name
}

const ossKeySeparator = "/"

func (info *OSSInfo) uniqueBucket() string {
	return fmt.Sprintf("%s-%s-unique", info.BucketName, info.Prefix)
}

func (info *OSSInfo) ProofBucket() string {
	if info.UniqueBucket {
		return info.uniqueBucket()
	} else {
		return fmt.Sprintf("%s-%s-proof", info.BucketName, info.Prefix)
	}
}

func (info *OSSInfo) DataBucket() string {
	if info.UniqueBucket {
		return info.uniqueBucket()
	} else {
		return fmt.Sprintf("%s-%s-data", info.BucketName, info.Prefix)
	}
}

func (info *OSSInfo) Equal(another *OSSInfo) bool {
	return info.URL == another.URL &&
		info.AccessKey == another.AccessKey &&
		info.SecretKey == another.SecretKey &&
		info.BucketName == another.BucketName &&
		info.Prefix == another.Prefix
}

func baseOSSClient(info StorageOSSInfo) (*OSSClient, error) {
	sess, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(info.AccessKey, info.SecretKey, ""),
		Endpoint:         aws.String(info.URL),
		Region:           aws.String(info.Region),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	})

	if err != nil {
		return nil, err
	}

	cli := s3.New(sess)

	ossCli := &OSSClient{
		s3Client:  cli,
		s3Session: sess,
		s3Info:    info,
	}

	return ossCli, nil
}

func NewOSSClientWithSingleBucket(info StorageOSSInfo) (*OSSClient, error) {
	ossCli, err := baseOSSClient(info)
	if err != nil {
		return nil, err
	}

	ossCli.proofBucket = info.BucketName
	ossCli.dataBucket = info.BucketName

	buckets, err := ossCli.s3Client.ListBuckets(nil)

	if err != nil {
		return nil, err
	}

	log.Debugf("buckets from %v", info.URL)
	log.Debugf("%v", buckets)

	for _, bucket := range buckets.Buckets {
		if *bucket.Name == info.BucketName {
			ossCli.s3Uploader = s3manager.NewUploader(ossCli.s3Session, func(u *s3manager.Uploader) {
				u.PartSize = int64(info.UploadPartSize)
			})
			ossCli.s3Downloader = s3manager.NewDownloader(ossCli.s3Session)
			return ossCli, nil
		}
	}

	return nil, xerrors.Errorf("bucket %v is not exist", info.BucketName)
}

func NewOSSClient(info StorageOSSInfo) (*OSSClient, error) {
	ossCli, err := baseOSSClient(info)
	if err != nil {
		return nil, err
	}

	ossCli.proofBucket = info.ProofBucket()
	ossCli.dataBucket = info.DataBucket()

	buckets, err := ossCli.s3Client.ListBuckets(nil)

	if err != nil {
		return nil, err
	}

	log.Debugf("buckets from %v", info.URL)
	log.Debugf("%v", buckets)

	bucketExists := false
	bucketName := info.ProofBucket()

	for _, bucket := range buckets.Buckets {
		if *bucket.Name == bucketName {
			bucketExists = true
			break
		}
	}

	if !bucketExists {
		return nil, fmt.Errorf("bucket %v is not exists", ossCli.proofBucket)
	}

	if !info.UniqueBucket {
		bucketExists = false
		bucketName = info.DataBucket()

		for _, bucket := range buckets.Buckets {
			if *bucket.Name == bucketName {
				bucketExists = true
				break
			}
		}

		if !bucketExists {
			return nil, fmt.Errorf("bucket %v is not exists", ossCli.dataBucket)
		}
	}

	ossCli.s3Uploader = s3manager.NewUploader(ossCli.s3Session, func(u *s3manager.Uploader) {
		u.PartSize = int64(info.UploadPartSize)
	})
	ossCli.s3Downloader = s3manager.NewDownloader(ossCli.s3Session)

	return ossCli, nil
}

func (oss *OSSClient) createBucket(bucketName string) error {
	_, err := oss.s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return err
	}

	err = oss.s3Client.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return err
	}

	return nil
}

func (oss *OSSClient) BucketNameByPrefix(prefix string) (string, error) {
	switch prefix {
	case "cache":
		return oss.proofBucket, nil
	case "sealed":
		return oss.dataBucket, nil
	case "unsealed":
		return oss.dataBucket, nil
	}
	return "", xerrors.Errorf("invalid prefix value %v", prefix)
}

func (oss *OSSClient) ListSectors(prefix string) ([]OSSSector, error) {
	bucketName, err := oss.BucketNameByPrefix(prefix)
	if err != nil {
		return nil, err
	}

	var startMarker *string = nil
	ossObjs := []OSSSector{}
	sectorFind := map[string]struct{}{}

	for {
		objs, err := oss.s3Client.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
			Prefix: aws.String(prefix),
			Marker: startMarker,
		})
		if err != nil {
			return nil, err
		}

		log.Infof("OSS: find %v sectors in %v/%v/%v", len(objs.Contents), oss.s3Info.URL, bucketName, prefix)

		for _, obj := range objs.Contents {
			keys := strings.Split(*obj.Key, ossKeySeparator)
			if len(keys) < 2 {
				return nil, xerrors.Errorf("error key %v from bucket %v", obj.Key, bucketName)
			}
			sectorName := keys[1]
			if _, ok := sectorFind[sectorName]; ok {
				continue
			}
			ossObjs = append(ossObjs, OSSSector{
				name: sectorName,
			})
			sectorFind[sectorName] = struct{}{}
		}

		startMarker = objs.NextMarker
		if !*objs.IsTruncated {
			break
		}
	}

	return ossObjs, nil
}

func (oss *OSSClient) UploadObject(prefix string, objName string, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	bucketName, err := oss.BucketNameByPrefix(prefix)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%v%v%v", prefix, ossKeySeparator, objName)

	_, err = oss.s3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		return err
	}

	err = oss.s3Client.WaitUntilObjectExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}

	return nil
}

func (oss *OSSClient) HeadObject(prefix string, objName string) (int64, error) {
	bucketName, err := oss.BucketNameByPrefix(prefix)
	if err != nil {
		return 0, err
	}

	key := fmt.Sprintf("%v%v%v", prefix, ossKeySeparator, objName)

	head, err := oss.s3Client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, err
	}

	if head.ContentLength == nil {
		return 0, xerrors.Errorf("content length is not available for %v", key)
	}

	return *head.ContentLength, nil
}
