package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sod-auctions/auctions-db"
	"io"
	"log"
	"net/url"
	"os"
	"strconv"
)

func init() {
	log.SetFlags(0)
}

func download(ctx context.Context, record *events.S3EventRecord, key string) (*s3.GetObjectOutput, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	s3Client := s3.New(sess)

	input := &s3.GetObjectInput{
		Bucket: aws.String(record.S3.Bucket.Name),
		Key:    aws.String(key),
	}

	return s3Client.GetObjectWithContext(ctx, input)
}

func parseIntOrCrash(s string, base int, bitSize int) int64 {
	result, err := strconv.ParseInt(s, base, bitSize)
	if err != nil {
		log.Fatalf("could not parse integer '%s'", s)
	}
	return result
}

func mapRowToPriceDistribution(row []string) *auctions_db.PriceDistribution {
	return &auctions_db.PriceDistribution{
		RealmID:        int16(parseIntOrCrash(row[0], 10, 16)),
		AuctionHouseID: int16(parseIntOrCrash(row[1], 10, 16)),
		ItemID:         int32(parseIntOrCrash(row[2], 10, 32)),
		BuyoutEach:     int32(parseIntOrCrash(row[3], 10, 32)),
		Quantity:       int32(parseIntOrCrash(row[4], 10, 32)),
	}
}

func handler(ctx context.Context, event events.S3Event) error {
	database, err := auctions_db.NewDatabase(os.Getenv("DB_CONNECTION_STRING"))
	if err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}

	for _, record := range event.Records {
		key, err := url.QueryUnescape(record.S3.Object.Key)
		if err != nil {
			return fmt.Errorf("error decoding S3 object key: %v", err)
		}

		log.Printf("downloading file %s", key)
		file, err := download(ctx, &record, key)
		if err != nil {
			return fmt.Errorf("error downloading file: %v", err)
		}
		defer file.Body.Close()

		log.Printf("reading price distributions from file..")
		r := csv.NewReader(file.Body)
		_, err = r.Read()
		if err != nil {
			return fmt.Errorf("failed to read CSV header: %v", err)
		}

		var priceDistributions []*auctions_db.PriceDistribution
		for {
			row, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("error reading CSV file: %v", err)
			}
			priceDistributions = append(priceDistributions, mapRowToPriceDistribution(row))
		}

		log.Printf("writing %d price distributions to database\n", len(priceDistributions))
		err = database.ReplacePriceDistributions(priceDistributions)
		if err != nil {
			return fmt.Errorf("error inserting price distributions into database: %v", err)
		}
	}
	return nil
}

func main() {
	lambda.Start(handler)
}
