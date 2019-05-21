[codecov-link]: https://codecov.io/gh/statisticsnorway/java-vtl/branch/develop
[travis-link]: https://travis-ci.org/statisticsnorway/java-vtl/branches
[gitter-link]: https://gitter.im/java-vtl/Lobby?utm_source=share-link&utm_medium=link&utm_campaign=share-link

[codecov-img]: https://codecov.io/gh/statisticsnorway/java-vtl/branch/develop/graph/badge.svg
[travis-img]: https://img.shields.io/travis/statisticsnorway/java-vtl/develop.svg
[gitter-img]: https://img.shields.io/gitter/room/java-vtl/Lobby.svg

[![Build Status][travis-img]][travis-link]
[![Codecov coverage][codecov-img]][codecov-link]
[![Gitter][gitter-img]][gitter-link]

# Java VTL: Java implementation of VTL

The Java VTL project is an open source java implementation of the
[VTL 1.1 draft specification](https://sdmx.org/?page_id=5096). It follows
the JSR-223 Java Scripting API and exposes a simple connector interface
one can implement in order to integrate with any data stores.

Visit the [interactive reference manual](https://statisticsnorway.github.io/java-vtl/reference/) for more information.

## Modules

The project is divided in modules;

- java-vtl-parent
    - java-vtl-parser, contains the lexer and parser for VTL.
    - java-vtl-model, VTL data model.
    - java-vtl-script, JSR-223 (ScriptEngine) implementation.
    - java-vtl-connector, connector API.
    - java-vtl-tools, various tools.

## Usage

Add a dependency to the maven project

```xml
<dependency>
    <groupId>no.ssb.vtl</groupId>
    <artifactId>java-vtl-script</artifactId>
    <version>0.1.13-SNAPSHOT</version>
</dependency>
```

## Evaluate VTL expressions

```java
ScriptEngine engine = new VTLScriptEngine(connector);

Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
engine.eval("ds1 := get(\"foo\")" +
            "ds2 := get(\"bar\")" +
            "ds3 := [ds1, ds2] {" +
            "   filter ds1.id = \"string\"," +
            "   total := ds1.measure + ds2.measure" +
            "}");

System.out.println(bindings.get("ds3"))
```

## Connect to external systems

VTL Java uses the `no.ssb.vtl.connector.Connector` interface to access and
export data from and to external systems.

The Connector interface defines three methods:

```java
public interface Connector {

    boolean canHandle(String identifier);

    Dataset getDataset(String identifier) throws ConnectorException;

    Dataset putDataset(String identifier, Dataset dataset) throws ConnectorException;

}
```

The method `canHandle(String identifier)` is used by the engine to find
 which connector is able to provide a Dataset for a given identifier.

The method `getDataset(String identifier)` is then called to get the dataset.
Example implementations can be found in the `java-vtl-ssb-api-connector` module
but a very crude implementation could be as such:

```java
class StaticDataset implements Dataset {

    private final DataStructure structure = DataStructure.builder()
            .put("id", Role.IDENTIFIER, String.class)
            .put("period", Role.IDENTIFIER, Instant.class)
            .put("measure", Role.MEASURE, Long.class)
            .put("attribute", Role.ATTRIBUTE, String.class)
            .build();

    @Override
    public Stream<DataPoint> getData() {

        List<Map<String, Object>> data = new ArrayList<>();
        HashMap<String, Object> row = new HashMap<>();
        Instant period = Instant.now();
        for (int i = 0; i < 100; i++) {
            row.put("id", "id #" + i);
            row.put("period", period);
            row.put("measure", Long.valueOf(i));
            row.put("attribute", "attribute #" + i);
            data.add(row);
        }

        return data.stream().map(structure::wrap);
    }

    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        return Optional.empty();
    }

    @Override
    public Optional<Long> getSize() {
        return Optional.of(100L);
    }

    @Override
    public DataStructure getDataStructure() {
        return structure;
    }
}
```

# Implementation roadmap

This is an overview of the implementation progress.

[done]: http://progressed.io/bar/100?title=done "Done"
[todo]: http://progressed.io/bar/0 "Done"

Group|Operators|Progress|Comment
---|---|---|---
General purpose|round parenthesis|![done][done]
General purpose|:= (assignment)|![done][done]
General purpose|membership|![done][done]
General purpose|get|![usable](http://progressed.io/bar/70)|The keep, filter and aggregate options are not implemented.
General purpose|put|![usable](http://progressed.io/bar/20)|Defined in the grammar but not implemented
Join expression|[]{}|![done][done]
Join clause|filter|![done][done]
Join clause|keep|![done][done]
Join clause|drop|![done][done]
Join clause|fold|![done][done]
Join clause|unfold|![done][done]
Join clause|rename|![done][done]
Join clause|:= (assignment)|![done][done]
Join clause|. (membership)|![done][done]
Clauses|rename|![done][done]
Clauses|filter|![done][done]
Clauses|keep|![done][done]
Clauses|calc|![todo][todo]
Clauses|attrcalc|![todo][todo]
Clauses|aggregate|![todo][todo]
Conditional|if-then-else|![todo][todo]
Conditional|nvl|![done][done]
Validation|Comparisons (>,<,>=,<=,=,<>)|![done][done]
Validation|in,not in, between|![todo][todo]
Validation|isnull|![done][done]|Implemented syntax are `isnull(value)`, `value is null` and `value is not null`|
Validation|exist_in, not_exist_in|![todo][todo]
Validation|exist_in_all, not_exist_in_all|![todo][todo]
Validation|check|![usable](http://progressed.io/bar/50)|The boolean dataset must be built manually (no lifting).
Validation|match_characters|![todo][todo]
Validation|match_values|![todo][todo]
Statistical|min, max|![todo][todo]
Statistical|hierarchy|![usable](http://progressed.io/bar/80)|The inline definition is not supported. A dataset that has a correct structure can be used instead.
Statistical|aggregate|![todo][todo]
Relational|union|![done][done]
Relational|intersect||![todo][todo]
Relational|symdiff|![todo][todo]
Relational|setdiff|![done][done]
Relational|merge|![todo][todo]
Boolean|and|![usable](http://progressed.io/bar/80)|Only inside join expression (no lifting).
Boolean|or|![usable](http://progressed.io/bar/80)|Only inside join expression (no lifting).
Boolean|xor|![usable](http://progressed.io/bar/80)|Only inside join expression (no lifting).
Boolean|not|![usable](http://progressed.io/bar/80)|Only inside join expression (no lifting).
Mathematical|unary plus and minus|![done][done]
Mathematical|addition, substraction|![done][done]
Mathematical|multiplication, division|![done][done]
Mathematical|round, ceil, floor|![done][done]
Mathematical|abs|![done][done]
Mathematical|trunc|![done][done]
Mathematical|power, exp, nroot|![done][done]
Mathematical|ln, log|![done][done]
Mathematical|mod|![done][done]
String|length|![todo][todo]
String|concatenation|![done][done]
String|trim|![todo][todo]
String|upper/lower case|![todo][todo]
String|substr|![usable](http://progressed.io/bar/80)|No lifting.
String|indexof|![todo][todo]
String|date_from_string|![usable](http://progressed.io/bar/25)|Dataset as input not implemented. Only YYYY date format accepted.
Outside specification|integer_from_string|![done][done]
Outside specification|float_from_string|![done][done]
Outside specification|string_from_number|![done][done]

[![Analytics](https://ga-beacon.appspot.com/UA-85245041-2/readme)](https://github.com/igrigorik/ga-beacon)


















