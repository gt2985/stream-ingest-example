package com.gcp.apps.util;

import org.apache.beam.sdk.extensions.gcp.auth.NullCredentialInitializer;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.util.Transport;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.pubsub.Pubsub;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableList;

public class GCPUtil {
	
	public static Bigquery.Builder newBQClient(BigQueryOptions options) {
		return new Bigquery.Builder(Transport.getTransport(), Transport.getJsonFactory(),
				chainHttpRequestInitializer(
						options.getGcpCredential(),
						// Do not log 404. It clutters the output and is possibly even required by the caller.
						new RetryHttpRequestInitializer(ImmutableList.of(404))))
				.setApplicationName(options.getAppName())
				.setGoogleClientRequestInitializer(options.getGoogleApiTrace());
	}

	private static HttpRequestInitializer chainHttpRequestInitializer(
			Credentials credential, HttpRequestInitializer httpRequestInitializer) {
		if (credential == null) {
			return new ChainingHttpRequestInitializer(
					new NullCredentialInitializer(), httpRequestInitializer);
		} else {
			return new ChainingHttpRequestInitializer(
					new HttpCredentialsAdapter(credential),
					httpRequestInitializer);
		}
	}

	/**
	 * Returns a Pubsub client builder using the specified {@link PubsubOptions}.
	 */
	public static Pubsub.Builder newPubsubClient(PubsubOptions options) {
		return new Pubsub.Builder(Transport.getTransport(), Transport.getJsonFactory(),
				chainHttpRequestInitializer(
						options.getGcpCredential(),
						// Do not log 404. It clutters the output and is possibly even required by the caller.
						new RetryHttpRequestInitializer(ImmutableList.of(404))))
				.setRootUrl(options.getPubsubRootUrl())
				.setApplicationName(options.getAppName())
				.setGoogleClientRequestInitializer(options.getGoogleApiTrace());
	}
	
	
}
