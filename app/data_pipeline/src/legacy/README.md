# Legacy Compatibility Code

Code in this namespace is retained only to support explicit migration or
historical jobs. It is not registered as the current K12 Stage 1 pipeline.

Any new caller must use a fully qualified `legacy.*` import so the dependency
is visible during review. Removal should happen only after all historical job
definitions and stored run configurations have been retired.

