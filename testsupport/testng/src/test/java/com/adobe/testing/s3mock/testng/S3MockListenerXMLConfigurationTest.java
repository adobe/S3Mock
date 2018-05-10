/*
 *  Copyright 2017-2018 Adobe.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.adobe.testing.s3mock.testng;

import com.adobe.testing.s3mock.util.HashUtil;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Test
public class S3MockListenerXMLConfigurationTest {
    private static final String BUCKET_NAME = "mydemotestbucket";
    private static final String UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt";

    private final AmazonS3 s3Client = S3Mock.getInstance().createS3Client("us-west-2");
    /**
     * Creates a bucket, stores a file, downloads the file again and compares checksums.
     * @throws Exception if FileStreams can not be read
     */
    @Test
    public void shouldUploadAndDownloadObject() throws Exception {
        final File uploadFile = new File(UPLOAD_FILE_NAME);

        s3Client.createBucket(BUCKET_NAME);
        s3Client.putObject(new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile));

        final S3Object s3Object = s3Client.getObject(BUCKET_NAME, uploadFile.getName());

        final InputStream uploadFileIS = new FileInputStream(uploadFile);
        final String uploadHash = HashUtil.getDigest(uploadFileIS);
        final String downloadedHash = HashUtil.getDigest(s3Object.getObjectContent());
        uploadFileIS.close();
        s3Object.close();

        Assert.assertEquals(uploadHash, downloadedHash, "Up- and downloaded Files should have equal Hashes");
    }

    /**
     * Creates a bucket, stores a file, adds tags, retrieves tags and checks them for consistency.
     */
    @Test
    public void shouldAddAndRetrieveTags() {
        final File uploadFile = new File(UPLOAD_FILE_NAME);

        s3Client.createBucket(BUCKET_NAME);
        s3Client.putObject(new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile));

        final S3Object s3Object = s3Client.getObject(BUCKET_NAME, uploadFile.getName());

        GetObjectTaggingRequest getObjectTaggingRequest = new GetObjectTaggingRequest(BUCKET_NAME,s3Object.getKey());
        GetObjectTaggingResult getObjectTaggingResult = s3Client.getObjectTagging(getObjectTaggingRequest);

        // There shouldn't be any tags here
        Assert.assertEquals(getObjectTaggingResult.getTagSet().size(),0);

        List<Tag> tagList = new ArrayList<>();
        tagList.add(new Tag("foo","bar"));

        final SetObjectTaggingRequest setObjectTaggingRequest = new SetObjectTaggingRequest(BUCKET_NAME,s3Object.getKey(),new ObjectTagging(tagList));
        final SetObjectTaggingResult setObjectTaggingResult = s3Client.setObjectTagging(setObjectTaggingRequest);

        getObjectTaggingRequest = new GetObjectTaggingRequest(BUCKET_NAME,s3Object.getKey());
        getObjectTaggingResult = s3Client.getObjectTagging(getObjectTaggingRequest);

        // There should be 'foo:bar' here
        Assert.assertEquals(getObjectTaggingResult.getTagSet().size(),1);
        Assert.assertEquals(getObjectTaggingResult.getTagSet().get(0).getValue(),"bar");
    }
}