package com.example.batchupload.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

import java.io.InputStream;

/**
 * Provides access to the input file stored in S3.
 * Uses S3's native byte-range support so each K8s pod can fetch only its slice.
 */
@Service
public class S3FileService {

    private final S3Client s3Client;

    public S3FileService(@Value("${aws.s3.region}") String region) {
        this.s3Client = S3Client.builder()
                .region(Region.of(region))
                .build();
    }

    /** Returns the total size of the S3 object in bytes. */
    public long getFileSize(String bucket, String key) {
        return s3Client.headObject(HeadObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build()).contentLength();
    }

    /**
     * Returns an InputStream for the specified byte range of the S3 object.
     *
     * @param bucket    S3 bucket name
     * @param key       S3 object key (file path)
     * @param startByte inclusive start offset
     * @param endByte   exclusive end offset; pass -1 or file-size to read to EOF
     */
    public InputStream getInputStream(String bucket, String key, long startByte, long endByte) {
        String range = (endByte <= startByte)
                ? String.format("bytes=%d-", startByte)
                : String.format("bytes=%d-%d", startByte, endByte - 1); // S3 range is inclusive

        return s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .range(range)
                .build());
    }
}
