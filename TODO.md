# What to work on next

The general structure of the package is now at a good point. It's not feature complete, and it's not extensively tested.
Given that we expect to use this package for the forseeable future, there are a few things I'd like to work on to make the foundation as solid as the time we have to build it allows.

- [ ] Structure a proper test suite that takes care of all the boilerplate, so that writing both unit and integration tests is easy
- [ ] Setup tox so we can run tests for all the supported python versions
- [ ] Use pydantic models to validate and serialize/deserialize requests, both for soundness and performance reasons.
- [ ] Setup a higher level api package (see examples/sdk.py as a POC) where we'll work on things that are outside the dbapi compliance stuff, like compute pool management etc.

Once those things are in place, adding features, testing existing ones, or fixing the exposed apis when we start building the dbt adapter and realize some things might have to be done differently should all be easier.

## Known missing features
- [ ] Handle parameters in queries. We need a way to sanitize query parameters before applying them to the query string (pydantic might help here)
