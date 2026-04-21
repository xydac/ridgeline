// Command ridgeline is the self-hosted intelligence platform binary.
//
// In cycle 1 this is a placeholder that prints version information.
// Subcommands (sync, status, query, daemon) ship in later cycles.
package main

import (
	"fmt"
	"os"
)

// Version is the build version. Overridden at release time via -ldflags.
var Version = "0.0.0-dev"

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "version", "--version", "-v":
			fmt.Println(Version)
			return
		}
	}
	fmt.Printf("ridgeline %s\n", Version)
	fmt.Println("Self-hosted intelligence platform. Subcommands coming soon.")
}
