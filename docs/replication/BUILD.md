# Building the appendix

The repository uses a project-local pinned Tectonic binary under `.tools/tectonic/` (ignored by Git). From `docs/replication`, run:

```powershell
..\..\.tools\tectonic\tectonic.exe appendix.tex --outdir .
```

The versioned output is `appendix.pdf`. The executable version and SHA-256 are recorded in `tectonic_manifest.json`.
