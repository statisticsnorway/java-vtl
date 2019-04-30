# Changelog 

## develop

### Added

* Add filter propagation logic
* Add filter propagation on join operations

### Changed

* Handle datasets with unequal attribute variables in union operations. All attributes will be 
kept and given NULL value if not present in source dataset
* CommonIdentifierBindings now doesn’t have bindings to the datasets, only the identifier keys. This means that dataset prefix in the ‘on’ clause in join operations is no longer allowed. This makes a more strict VTL parsing, so one can only 
use common identifiers, and not just any identifier.
* Change inner and outer joins as described in the VTL 1.1 specification (1810-1818).

## 0.1.12-2 - 2019-03-21

### Changed

* Update project dependencies

## 0.1.12-1 - 2019-03-05

### Changed 

* Update URLs to internal SSB distribution repos

## 0.1.12 - 2018-11-21

### Changed

* Update outdated dependencies
* Remove hamcrest dependencies
* Hierarchy operator handles null values in complement relations
* Fix a bug in outer join expression with more than two datasets

## 0.1.11 - 2018-10-23

### Added

* Experimental foreach operator (#96)
* Supports escaped variable names
* Throw ContextualRuntimeException in user function visitor 

## 0.1.10-3 - 2018-09-19

### Changed

* Fix a bug in InnerJoinOperation when requested order does not include all the identifiers

## 0.1.10-2 - 2018-09-04

### Changed

* Fix invalid key extraction in InnerJoinOperation

## 0.1.10-1 - 2018-08-31

### Added 

* Fix missing jmh dependencies

## 0.1.10 - 2018-08-31

### Added

* Support for average aggregation function `ds := avg(ds1) group by ds1.x`
* Attribute components are now kept when using `fold`
* Fold optimization
* Expose keyword list in `VTLScriptEngine`

## 0.1.9-2 - 2018-06-01

### Changed

* Force type casting to ensure correct return type when VTLFloor gets input of type Integer.

## 0.1.9-1 - 2018-05-30

### Changed

* Floor function returns null when given a non finite value

## 0.1.9 - 2018-05-23

### Added

* This changelog
* HierarchyOperation cache the graph representation of the hierarchy dataset.
* InnerJoinOperation tries to forward the requested order to its children

### Changed

* Add support non finite values in `round` and `floor` functions
* Hierarchy operation does not call `Dataset#getData()` on hierarchy dataset until `HierarchyOperation#getData()` method is called.
* `InnerJoinOperation` does not try to output Cartesian product of misses and instead simply clear its buffer.
* `InnerJoinSpliterator` now fully respect the `Spliterator` API.

##  0.1.8 - 2018-05-07

## 0.1.7 - 2018-04-25

## 0.1.6 - 2018-04-12



```bash
git log 0.1.8..HEAD --oneline --decorate --color --first-parent
```



