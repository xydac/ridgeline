// Package posthog implements a native connector for PostHog self-hosted
// and cloud analytics.
//
// The connector syncs the "events" stream, which produces one record per
// event with typed timestamp, event name, and distinct_id columns. All
// other event properties are available in the data_json payload.
//
// Auth: Personal API Key (https://app.posthog.com/settings/user-api-keys).
// Set api_key in config or via api_key_ref pointing to a stored credential.
//
// Example ridgeline.yaml excerpt:
//
//	products:
//	  - name: myapp
//	    connectors:
//	      - type: posthog
//	        project_id: "12345"
//	        api_key_ref: posthog.myapp.api_key
//	        streams:
//	          - name: events
package posthog
