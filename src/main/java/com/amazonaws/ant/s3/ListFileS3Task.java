/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.ant.s3;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.HashSet;

import org.apache.tools.ant.BuildException;

import com.amazonaws.ant.AWSAntTask;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Ant task for Downloading a file from a specified bucket in S3. You have two
 * main options for downloading. You may either specify a key and download the
 * file in your bucket in S3 with that key (To a target file if you wish to
 * specify it), or specify a prefix and download all files from your bucket in
 * S3 with that prefix to a specified directory.
 */
public class ListFileS3Task extends AWSAntTask {
    private String bucketName;
    private String prefix = "";
    private boolean recursive = true;

    /**
     * Specify the name of your S3 bucket
     * 
     * @param bucketName
     *            The name of the bucket in S3 to download the file from. An
     *            exception will be thrown if it doesn't exist.
     */
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    /**
     * Specify your key prefix. If set, you must also specify a directory.
     * 
     * @param prefix
     *            All files in your bucket in S3 whose keys begin with these
     *            prefix will be downloaded to a specified directory
     */
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    /**
     * If you download files based on a prefix, then you must specify a
     * directory to download the files to.
     *
     * @param recursive
     *            Whether we should recursively list files
     */
    public void setRecursive(Boolean recursive) {
        this.recursive = recursive;

    }

    public void checkParams() {
        boolean areMalformedParams = false;
        StringBuilder errors = new StringBuilder("");
        if (bucketName == null) {
            areMalformedParams = true;
            errors.append("Missing parameter: bucketName is required. \n");
        }
        if (areMalformedParams) {
            throw new BuildException(errors.toString());
        }
    }

    public void execute() {
        AmazonS3Client client = getOrCreateClient(AmazonS3Client.class);
            ObjectListing objectListing = client.listObjects(bucketName,prefix);

	    HashSet<String> seenSet = new HashSet<String>();

            while (true) {
                for (Iterator<?> iterator = objectListing.getObjectSummaries()
                        .iterator(); iterator.hasNext();) {
                    S3ObjectSummary objectSummary = (S3ObjectSummary) iterator
                            .next();
                    String key = objectSummary.getKey();
		    String output = key;
                    if (prefix != null && key.startsWith(prefix)) {
			output = key.substring(prefix.length());
	            }
                    if(recursive)
	            {	
                           System.out.println(output);       
			}
			else
			{
			   String file = output.split("/")[0];
		           if(seenSet.add(file))
                             System.out.println(file);
			}
                }

                if (objectListing.isTruncated()) {
                    objectListing = client
                            .listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }
    }
}
