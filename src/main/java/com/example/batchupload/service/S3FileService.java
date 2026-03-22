package com.example.batchupload.service;

import com.example.batchupload.config.AppProperties;
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
    private final String bucket;
    private final String key;

    public S3FileService(AppProperties props) {
        AppProperties.S3 s3Props = props.s3();
        this.bucket = s3Props.bucket();
        this.key = s3Props.key();
        this.s3Client = S3Client.builder()
                .region(Region.of(s3Props.region()))
                .build();
    }

    /** Returns the total size of the S3 object in bytes. */
    public long getFileSize() {
        return s3Client.headObject(HeadObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build()).contentLength();
    }

    /**
     * Returns an InputStream for the specified byte range of the S3 object.
     *
     * @param startByte inclusive start offset
     * @param endByte   exclusive end offset; pass -1 or file-size to read to EOF
     */
    public InputStream getInputStream(long startByte, long endByte) {
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
