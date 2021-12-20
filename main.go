package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ファイルアップロードGoroutineの同時実行上限数
const limit = 3

const settingsFileName = "settings.json"

var Warn = errors.New("")

type settingsFile struct {
	AwsAccessKeyId      string
	AwsSecretAccessKey  string
	AwsRegion           string
	S3Bucket            string
	S3DirPath           string
	TargetFilesDirPath  string
	TargetDirNameRegExp string
	ProxyUrl            string
	S3PutTimeoutSeconds int64
}

type awsConfig struct {
	AwsAccessKeyId     string
	AwsSecretAccessKey string
	AwsRegion          string
}

type requiredSettings struct {
	S3Bucket           string
	S3DirPath          string
	TargetFilesDirPath string
}

type optionalSettings struct {
	TargetDirNameRegExp *regexp.Regexp
	ProxyUrl            string
	S3PutTimeoutSeconds int64
}

var verbose bool

func init() {
	log.SetOutput(os.Stdout)
}

func main() {
	ctx := context.Background()
	c := process(ctx)
	os.Exit(c)
}

func process(ctx context.Context) int {
	log.Println("[INFO] Files transfer process start.")
	defer log.Println("[INFO] Files transfer process end.")

	hasError := false
	hasWarn := false

	awsConfig, requiredSettings, opionalSettings, err := loadSettings()
	if err != nil {
		log.Printf("[ERROR] Failed to load settings. Give up to upload all target files.: %v", err)
		return 9
	}

	client, err := createS3Client(ctx, awsConfig, opionalSettings)
	if err != nil {
		log.Printf("[ERROR] Failed to create S3Client. Give up to upload all target files.: %v", err)
		return 9
	}

	subDirNames, err := uploadTargetFiles(ctx, client, requiredSettings, opionalSettings)
	if err != nil {
		if errors.Is(err, Warn) {
			hasWarn = true
		} else {
			hasError = true
			if err.Error() != "" {
				log.Printf("[ERROR] Failed to upload all target files: %v", err)
			}
		}
	} else {
		log.Printf("[INFO] All target files in %v were successfully uploaded.", requiredSettings.TargetFilesDirPath)
	}

	// アップロード成否は問わず、アップロード処理が終わったことを通知するための空ファイルを各サブディレクトリへアップロードする
	if err = uploadFileForNoticeToEndUpload(ctx, client, subDirNames, requiredSettings, opionalSettings); err != nil {
		hasWarn = true
	}

	// TargetFilesDirPath 配下の空ディレクトリを削除（正常にアップロード、削除されたら空のサブディレクトリだけが残るので）
	if err = removeDirIfEmpty(requiredSettings.TargetFilesDirPath, subDirNames); err != nil {
		hasWarn = true
	}

	if hasError {
		return 9
	} else if hasWarn {
		return 1
	}
	return 0
}

func loadSettings() (awsConfig, requiredSettings, optionalSettings, error) {
	paramAwsAccessKeyId := flag.String("AwsAccessKeyId", "", "AWS access key id")
	paramAwsSecretAccessKey := flag.String("AwsSecretAccessKey", "", "AWS secret access key")
	paramAwsRegion := flag.String("AwsRegion", "", "AWS region")

	paramS3Bucket := flag.String("S3Bucket", "", "S3 bucket name")
	paramS3DirPath := flag.String("S3DirPath", "", "S3 bucket directory path")
	paramTargetFilesDirPath := flag.String("TargetFilesDirPath", "", "interface files directory path")
	paramTargetDirNameRegExp := flag.String("TargetDirNameRegExp", "", "regexp for target directory name")
	paramProxyUrl := flag.String("ProxyUrl", "", "proxy url. eg. http://myproxy:myport/")
	paramS3PutTimeoutSeconds := flag.Int64("S3PutTimeoutSeconds", 0, "timeout seconds during uploading per file")

	paramVerbose := flag.Bool("Verbose", false, "verbose mode. default: false")
	verbose = *paramVerbose

	paramSettings := flag.String("Settings", "", "path to settings.json file. default: [this executable file dir]/settings.json")

	flag.Parse()

	resultAwsConfig := awsConfig{
		AwsAccessKeyId:     *paramAwsAccessKeyId,
		AwsSecretAccessKey: *paramAwsSecretAccessKey,
		AwsRegion:          *paramAwsRegion,
	}
	resultRequiredSettings := requiredSettings{
		S3Bucket:           *paramS3Bucket,
		S3DirPath:          *paramS3DirPath,
		TargetFilesDirPath: *paramTargetFilesDirPath,
	}
	resultOpionalSettings := optionalSettings{
		ProxyUrl:            *paramProxyUrl,
		S3PutTimeoutSeconds: *paramS3PutTimeoutSeconds,
	}
	if *paramTargetDirNameRegExp != "" {
		resultOpionalSettings.TargetDirNameRegExp = regexp.MustCompile(*paramTargetDirNameRegExp)
	}

	var settingsPath string
	if *paramSettings == "" {
		thisDir, err := getExecutableDirPath()
		if err != nil {
			if resultRequiredSettings.S3Bucket == "" || resultRequiredSettings.S3DirPath == "" || resultRequiredSettings.TargetFilesDirPath == "" {
				return resultAwsConfig, resultRequiredSettings, resultOpionalSettings, fmt.Errorf("Failed to resolve path of settings.json: %w", err)
			}
			return resultAwsConfig, resultRequiredSettings, resultOpionalSettings, nil
		}
		settingsPath = filepath.Join(thisDir, settingsFileName)
	} else {
		settingsPath = *paramSettings
	}

	settingsFile, err := loadSettingsFile(settingsPath)
	if err != nil {
		if *paramSettings != "" || resultRequiredSettings.S3Bucket == "" || resultRequiredSettings.S3DirPath == "" || resultRequiredSettings.TargetFilesDirPath == "" {
			return resultAwsConfig, resultRequiredSettings, resultOpionalSettings, fmt.Errorf("Failed to load settings.json: %w", err)
		} else {
			return resultAwsConfig, resultRequiredSettings, resultOpionalSettings, nil
		}
	}
	resultAwsConfig.AwsAccessKeyId = coalesce(resultAwsConfig.AwsAccessKeyId, settingsFile.AwsAccessKeyId)
	resultAwsConfig.AwsSecretAccessKey = coalesce(resultAwsConfig.AwsSecretAccessKey, settingsFile.AwsSecretAccessKey)
	resultAwsConfig.AwsRegion = coalesce(resultAwsConfig.AwsRegion, settingsFile.AwsRegion)
	resultRequiredSettings.S3Bucket = coalesce(resultRequiredSettings.S3Bucket, settingsFile.S3Bucket)
	resultRequiredSettings.S3DirPath = coalesce(resultRequiredSettings.S3DirPath, settingsFile.S3DirPath)
	resultRequiredSettings.TargetFilesDirPath = coalesce(resultRequiredSettings.TargetFilesDirPath, settingsFile.TargetFilesDirPath)
	if resultOpionalSettings.TargetDirNameRegExp == nil && settingsFile.TargetDirNameRegExp != "" {
		resultOpionalSettings.TargetDirNameRegExp = regexp.MustCompile(settingsFile.TargetDirNameRegExp)
	}
	resultOpionalSettings.ProxyUrl = coalesce(resultOpionalSettings.ProxyUrl, settingsFile.ProxyUrl)
	if resultOpionalSettings.S3PutTimeoutSeconds == 0 {
		resultOpionalSettings.S3PutTimeoutSeconds = settingsFile.S3PutTimeoutSeconds
	}
	if resultRequiredSettings.S3Bucket == "" || resultRequiredSettings.S3DirPath == "" || resultRequiredSettings.TargetFilesDirPath == "" {
		return resultAwsConfig, resultRequiredSettings, resultOpionalSettings, errors.New("Required parameters of S3Bucket and S3DirPath and TargetFilesDirPath")
	}
	return resultAwsConfig, resultRequiredSettings, resultOpionalSettings, nil
}

func getExecutableDirPath() (string, error) {
	p, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Dir(p), nil
}

func coalesce(value string, defaultValue string) string {
	if value != "" {
		return value
	}
	return defaultValue
}

func loadSettingsFile(path string) (settingsFile, error) {
	var s settingsFile
	b, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return s, err
	}
	return s, nil
}

func createS3Client(ctx context.Context, awsConfig awsConfig, option optionalSettings) (*s3.Client, error) {
	var loadOpFns []func(o *config.LoadOptions) error
	var cfg aws.Config
	var err error

	if option.ProxyUrl != "" {
		httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
			proxy, err := url.Parse(option.ProxyUrl)
			if err != nil {
				log.Printf("[ERROR] Failed to parse proxyUrl %v: %v", option.ProxyUrl, err)
			}
			tr.Proxy = http.ProxyURL(proxy)
		})
		loadOpFns = append(loadOpFns, config.WithHTTPClient(httpClient))
	}
	if awsConfig.AwsAccessKeyId != "" && awsConfig.AwsSecretAccessKey != "" {
		loadOpFns = append(loadOpFns, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(awsConfig.AwsAccessKeyId, awsConfig.AwsSecretAccessKey, "")))
	}
	if awsConfig.AwsRegion != "" {
		loadOpFns = append(loadOpFns, config.WithRegion(awsConfig.AwsRegion))
	}
	// デフォルトで ~/.aws/config や、環境変数AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY、インスタンスプロファイルからクレデンシャル情報をロードしてくれるし、
	// 環境変数HTTP_PROXY/HTTPS_PROXYがあればその値をプロキシとして使ってくれる。
	cfg, err = config.LoadDefaultConfig(ctx, loadOpFns...)
	if err != nil {
		return nil, fmt.Errorf("Failed to load aws credential: %w", err)
	}
	return s3.NewFromConfig(cfg), nil
}

func compressFile(path string) (string, error) {
	isGzip := filepath.Ext(path) == ".gz"

	gzPath, err := func() (string, error) {
		file, err := os.Open(path)
		if err != nil {
			return "", fmt.Errorf("Failed to open the file %v: %w", path, err)
		}
		if isGzip {
			return file.Name(), nil
		}
		defer file.Close()

		gzPath := path + ".gz"
		dist, err := os.Create(gzPath)
		if err != nil {
			dist.Close()
			return "", fmt.Errorf("Failed to create the file %v: %w", gzPath, err)
		}
		zw, err := gzip.NewWriterLevel(dist, gzip.DefaultCompression)
		if err != nil {
			dist.Close()
			return "", fmt.Errorf("Failed to create gzip writer %v: %w", gzPath, err)
		}
		defer zw.Close()

		if _, err = io.Copy(zw, file); err != nil {
			dist.Close()
			return "", fmt.Errorf("Failed to write contents to the gzip file %v: %w", gzPath, err)
		}
		return dist.Name(), nil
	}()
	if err != nil {
		return "", err
	}
	if !isGzip {
		if err = os.Remove(path); err != nil {
			log.Printf("[WARN] Failed to remove the file %v: %v", path, err)
		}
	}
	return gzPath, nil
}

func uploadTargetFiles(ctx context.Context, c *s3.Client, settings requiredSettings, option optionalSettings) ([]string, error) {
	slots := make(chan struct{}, limit)

	subDirNames := []string{}

	wg := &sync.WaitGroup{}

	hasError := false
	var emu sync.Mutex
	hasWarn := false
	var wmu sync.Mutex

	// walkDir はレキシカルオーダーをもち、出力は決定論的なので、途中でファイルが追加されたりしても結果は変わらない。
	// また、コードを読んで確認した結果、root直下のディレクトリ・ファイルの一覧をディレクトリ・ファイル名の昇順でソートし、
	// 先頭から順に呼んでいく（対象がディレクトリの場合はその中を同様に昇順で処理してから次に進む）ので、
	// 例えば
	// xxx/
	//   20210101001021
	//     a
	//     b
	//   20210102001132
	//     a
	//     b
	// このような構成の場合、この通り上から順に処理される。
	err := filepath.WalkDir(settings.TargetFilesDirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// この分岐に入るのは、settings.TargetFilesDirPath に入れなかった場合と、
			// そのサブディレクトリについて、本コールバック関数の呼び出しが終わった後に、配下のファイル一覧を取得するためにサブディレクトリに入ろうとしてダメだった場合のみ
			// （後者の場合のみ、本コールバック関数は同一ディレクトリに対して2回呼ばれる）
			if path == settings.TargetFilesDirPath {
				// settings.TargetFilesDirPath に入れなかった場合は何もできないので、WalkDirを終了させるべくエラーを返す
				return fmt.Errorf("Failed to read the target root directory %v: %w", path, err)
			} else {
				hasError = true
				log.Printf("[ERROR] Failed to read the directory %v to upload: %v", path, err)
				return nil
			}
		}
		if d.IsDir() {
			if path == settings.TargetFilesDirPath {
				return nil
			}
			if !isTargetFilesDir(settings.TargetFilesDirPath, path, option.TargetDirNameRegExp) {
				return filepath.SkipDir
			}
			subDirNames = append(subDirNames, d.Name())
			return nil
		}

		gzPath, err := compressFile(path)
		if err != nil {
			hasError = true
			log.Printf("[ERROR] Failed to compress the file %v to upload: %v", path, err)
			return nil
		}
		rp, err := filepath.Rel(settings.TargetFilesDirPath, gzPath)
		if err != nil {
			hasError = true
			log.Printf("[ERROR] Failed to get relative path of the file %v to upload: %v", gzPath, err)
			return nil
		}

		key := filepath.ToSlash(filepath.Join(settings.S3DirPath, rp))

		wg.Add(1)
		slots <- struct{}{}
		go func() {
			defer wg.Done()
			gzip, err := os.Open(gzPath)
			if err != nil {
				emu.Lock()
				hasError = true
				emu.Unlock()
				log.Printf("[ERROR] Failed to upload file %v: %v", gzPath, err)
			}
			var r io.Reader = gzip
			err = uploadFile(ctx, c, settings.S3Bucket, key, r, option)
			gzip.Close()
			if err != nil {
				emu.Lock()
				hasError = true
				emu.Unlock()
				log.Printf("[ERROR] Failed to upload file %v: %v", gzPath, err)
			} else {
				if err := os.Remove(gzPath); err != nil {
					wmu.Lock()
					hasWarn = true
					wmu.Unlock()
					log.Printf("[WARN] Failed to remove file %v after uploaded: %v", gzPath, err)
				}
			}
			<-slots
		}()
		return nil
	})
	wg.Wait()
	if err != nil {
		return nil, err
	} else if hasError {
		return subDirNames, errors.New("")
	} else if hasWarn {
		return subDirNames, Warn
	}
	return subDirNames, nil
}

func isTargetFilesDir(srcDirPath string, dirPath string, targetDirName *regexp.Regexp) bool {
	if filepath.Clean(srcDirPath) != filepath.Dir(dirPath) {
		return false
	}
	if targetDirName == nil {
		return true
	}
	return targetDirName.MatchString(filepath.Base(dirPath))
}

func uploadFileForNoticeToEndUpload(ctx context.Context, c *s3.Client, subDirNames []string, setting requiredSettings, option optionalSettings) error {
	hasWarn := false

	for _, subDirName := range subDirNames {
		key := filepath.Join(setting.S3DirPath, subDirName, "upload_end_notice")
		var emptyReader io.Reader = strings.NewReader("")
		if err := uploadFile(ctx, c, setting.S3Bucket, key, emptyReader, option); err != nil {
			hasWarn = true
			log.Printf("[WARN] Failed to upload upload end notification file to %v: %v", key, err)
		}
	}
	if hasWarn {
		return Warn
	}
	return nil
}

func uploadFile(ctx context.Context, c *s3.Client, bucket string, key string, r io.Reader, option optionalSettings) error {
	// デフォルトのリトライの挙動は https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/retries-timeouts/ を参照
	ctxIn := ctx
	if option.S3PutTimeoutSeconds > 0 {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Duration(option.S3PutTimeoutSeconds)*time.Second)
		defer cancel()
		ctxIn = ctxWithTimeout
	}
	_, err := c.PutObject(ctxIn, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   r,
	})
	if verbose && err == nil {
		log.Printf("[INFO] Successfully uploaded to %v", key)
	}
	return err
}

func removeDirIfEmpty(parentDirPath string, childDirNames []string) error {
	hasWarn := false

	for _, childDirName := range childDirNames {
		path := filepath.Join(parentDirPath, childDirName)

		files, err := os.ReadDir(path)
		if err != nil {
			hasWarn = true
			log.Printf("[WARN] Failed to read the directory %v to remove: %v", path, err)
			continue
		}
		if len(files) == 0 {
			if err = os.Remove(path); err != nil {
				hasWarn = true
				log.Printf("[WARN] Failed to remove empty directory %v: %v", path, err)
			}
		}
	}
	if hasWarn {
		return Warn
	}
	return nil
}
