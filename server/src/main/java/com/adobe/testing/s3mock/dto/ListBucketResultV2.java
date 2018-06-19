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

package com.adobe.testing.s3mock.dto;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;

import com.adobe.testing.s3mock.domain.BucketContents;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import com.thoughtworks.xstream.annotations.XStreamInclude;

/**
 * Represents a result of listing objects that reside in a Bucket.
 */
@XStreamAlias("ListBucketResult")
@XStreamInclude({ BucketContents.class })
public class ListBucketResultV2 implements Serializable {
	@XStreamAlias("Name")
	private String name;

	@XStreamAlias("Prefix")
	private String prefix;

	@XStreamAlias("MaxKeys")
	private int maxKeys;

	@XStreamAlias("IsTruncated")
	private boolean isTruncated;

	@XStreamImplicit(itemFieldName = "Contents")
	private List<BucketContents> contents;

	@XStreamAlias("CommonPrefixes")
	private String commonPrefixes;

	@XStreamAlias("ContinuationToken")
	private String continuationToken;

	@XStreamAlias("KeyCount")
	private String keyCount;

	@XStreamAlias("NextContinuationToken")
	private String nextContinuationToken;

	@XStreamAlias("StartAfter")
	private String startAfter;

	/**
	 * Constructs a new {@link ListBucketResultV2}.
	 *
	 */
	public ListBucketResultV2() {
		// empty
	}

	/**
	 * Constructs a new {@link ListBucketResultV2}.
	 *
	 * @param name
	 *            {@link String}
	 * @param prefix
	 *            {@link String}
	 * @param maxKeys
	 *            {@link String}
	 * @param isTruncated
	 *            {@link Boolean}
	 * @param contents
	 *            {@link List}
	 * @param commonPrefixes
	 *            {@link String}
	 * @param continuationToken
	 *            {@link String}
	 * @param keyCount
	 *            {@link String}
	 * @param nextContinuationToken
	 *            {@link String}
	 * @param startAfter
	 *            {@link String}
	 */
	public ListBucketResultV2(final String name, final String prefix, final String maxKeys, final boolean isTruncated,
			final List<BucketContents> contents, final String commonPrefixes, final String continuationToken,
			final String keyCount, final String nextContinuationToken, final String startAfter) {
		super();
		this.name = name;
		this.prefix = prefix;
		this.maxKeys = Integer.valueOf(maxKeys);
		this.isTruncated = isTruncated;
		this.contents = new ArrayList<>();
		this.contents.addAll(contents);
		this.commonPrefixes = commonPrefixes;
		this.continuationToken = continuationToken;
		this.keyCount = keyCount;
		this.nextContinuationToken = nextContinuationToken;
		this.startAfter = startAfter;
	}

	@XmlElement(name = "Name")
	public String getName() {
		return name;
	}

	@XmlElement(name = "MaxKeys")
	public String getMaxKeys() {
		return String.valueOf(maxKeys);
	}

	@XmlElement(name = "IsTruncated")
	public boolean isTruncated() {
		return isTruncated;
	}

	public List<BucketContents> getContents() {
		return contents;
	}

	public void setContents(final List<BucketContents> contents) {
		this.contents = contents;
	}

	@XmlElement(name = "CommonPrefixes")
	public String getCommonPrefixes() {
		return commonPrefixes;
	}

	@XmlElement(name = "ContinuationToken")
	public String getContinuationToken() {
		return continuationToken;
	}

	@XmlElement(name = "KeyCount")
	public String getKeyCount() {
		return keyCount;
	}

	@XmlElement(name = "NextContinuationToken")
	public String getNextContinuationToken() {
		return nextContinuationToken;
	}

	@XmlElement(name = "StartAfter")
	public String getStartAfter() {
		return startAfter;
	}
}
