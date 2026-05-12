# SAST Audit Report

Found one confirmed vulnerability:

- Missing object-level authorization check in `SampleController.list_sample_runs` and `SampleController.list_sample_assemblies` permits unauthorized users to enumerate relationships for private samples.

