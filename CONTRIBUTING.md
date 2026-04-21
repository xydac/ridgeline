# Contributing

Thanks for your interest in Ridgeline. A few conventions keep the codebase consistent and the commit history readable.

## Before opening a PR

1. Sign the [CLA](CLA.md) by adding your name to `cla-signatures.md` in your first PR. This is one-time.
2. Run `gofmt -w .`, `go vet ./...`, and `go test ./...`. All must pass.
3. Each commit should be atomic: one logical change, tests passing at every point in the history.

## Commit style

- Imperative mood: "Add X", "Fix Y", "Refactor Z".
- No em-dashes. No AI-generated phrasing.
- Reference issues or decisions where relevant.
- No co-author tags.

## Code style

- Standard Go formatting (`gofmt`).
- `go vet` and `staticcheck` clean.
- Public types get doc comments. Exported interfaces get usage examples.
- Prefer small focused files over large multi-purpose ones.
- Tests alongside code.

## Writing a connector

See `docs/writing-a-connector.md` (coming soon) and existing connectors in `connectors/native/` or the external connector examples.

## License

By contributing, you agree your contributions are licensed under the MIT License and that you have the right to grant that license.
