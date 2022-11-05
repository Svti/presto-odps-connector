/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.odps.cupid.presto;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

import com.facebook.airlift.configuration.Config;

import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

public class OdpsConfig {
	private String project;
	private String accessId;
	private String accessKey;
	private String endpoint;
	private String tunnelEndpoint;
	private int splitSize;
	private List<String> extraProjectList = new ArrayList<>(2);
	private boolean caseInsensitiveNameMatching;
	private Duration caseInsensitiveNameMatchingCacheTtl = new Duration(1, MINUTES);

	public List<String> getExtraProjectList() {
		return extraProjectList;
	}

	public String getProject() {
		return project;
	}

	public String getAccessId() {
		return accessId;
	}

	public String getAccessKey() {
		return accessKey;
	}

	public String getEndPoint() {
		return endpoint;
	}

	public String getTunnelEndPoint() {
		return tunnelEndpoint;
	}

	public int getSplitSize() {
		return splitSize;
	}

	public boolean isCaseInsensitiveNameMatching() {
		return caseInsensitiveNameMatching;
	}

	@Config("odps.case-insensitive-name-matching")
	public OdpsConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching) {
		this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
		return this;
	}

	@NotNull
	@MinDuration("0ms")
	public Duration getCaseInsensitiveNameMatchingCacheTtl() {
		return caseInsensitiveNameMatchingCacheTtl;
	}

	@Config("odps.case-insensitive-name-matching.cache-ttl")
	public OdpsConfig setCaseInsensitiveNameMatchingCacheTtl(Duration caseInsensitiveNameMatchingCacheTtl) {
		this.caseInsensitiveNameMatchingCacheTtl = caseInsensitiveNameMatchingCacheTtl;
		return this;
	}

	@Config("odps.project.name.extra.list")
	public OdpsConfig setExtraProjectList(String projectList) {
		for (String prj : projectList.split(",")) {
			if (!prj.trim().isEmpty()) {
				this.extraProjectList.add(prj.trim());
			}
		}
		return this;
	}

	@Config("odps.project.name")
	public OdpsConfig setProject(String project) {
		this.project = project;
		return this;
	}

	@Config("odps.access.id")
	public OdpsConfig setAccessId(String accessId) {
		this.accessId = accessId;
		return this;
	}

	@Config("odps.access.key")
	public OdpsConfig setAccessKey(String accessKey) {
		this.accessKey = accessKey;
		return this;
	}

	@Config("odps.end.point")
	public OdpsConfig setEndPoint(String endpoint) {
		this.endpoint = endpoint;
		return this;
	}

	@Config("odps.tunnel.end.point")
	public OdpsConfig setTunnelEndPoint(String tunnelEndpoint) {
		this.tunnelEndpoint = tunnelEndpoint;
		return this;
	}

	@Config("odps.input.split.size")
	public OdpsConfig setSplitSize(int splitSize) {
		this.splitSize = splitSize;
		return this;
	}
}
