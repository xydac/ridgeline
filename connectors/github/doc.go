// Package github implements a Ridgeline connector for GitHub repository
// traffic data (views and clones) via the GitHub REST API.
//
// Authentication requires a GitHub personal access token (PAT) with at
// least read access to the target repository. Traffic data is only
// available to users with push access to the repository.
//
// Usage in ridgeline.yaml:
//
//	products:
//	  - name: myrepo
//	    connector: github
//	    config:
//	      owner: acme
//	      repo: widgets
//	      api_token_ref: github.myrepo.token
//	    streams:
//	      - name: views
//	      - name: clones
//
// Store the PAT with:
//
//	ridgeline creds put github.myrepo.token <token>
package github
