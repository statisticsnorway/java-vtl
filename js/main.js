require.config({
    paths: {
        //'react': '../../js/react',
        //'react-dom': '../../js/react-dom',
        //'react': 'https://unpkg.com/react@15/dist/react',
        //'react-dom': 'https://unpkg.com/react-dom@15/dist/react-dom',
        //'lodash.debounce': 'https://unpkg.com/lodash.debounce@4.0.8/index',
        //'classnames': 'https://unpkg.com/classnames@2.2.5/index',
        //'react-codemirror': 'https://unpkg.com/react-codemirror@0.3.0/dist/react-codemirror'
        //vtl: base_url + '/../../js/'

        'codemirror': '//unpkg.com/codemirror@5.23.0/lib/codemirror',
        'codemirror-simple': '//unpkg.com/codemirror@5.23.0/addon/mode/simple',
        'rd': base_url + '/../js/railroad-diagrams',
        'angular': '//unpkg.com/angular@1.6.1/angular',
        'smart-table': 'https://unpkg.com/angular-smart-table@2.1.8/dist/smart-table',
        'ui.bootstrap': 'https://unpkg.com/angular-ui-bootstrap@2.5.0/dist/ui-bootstrap-tpls',
        'ui.codemirror':'//unpkg.com/angular-ui-codemirror@0.3.0/src/ui-codemirror'
    },
    shim: {
        angular: {
            exports: 'angular'
        },
        'smart-table': {
            deps: ['angular']
        },
        'ui.bootstrap': {
            deps: ['angular']
        },
        'ui.codemirror': {
            deps: ['angular', 'codemirror'],
            init: function (angular, codemirror) {
                // VTL Codemirror setup.
                codemirror.defineSimpleMode("vtl", {
                    // The start state contains the rules that are initially used
                    start: [

                        {regex: /\/\*/, token: "comment", next: "comment", indent: true},
                        {regex: /"[^"]*"/, token: "string"},

                        {regex: /get|put|and|or/, token: "keyword"},

                        // Incomplete, escaped quotes ('') are not matched
                        {regex: /(:?[a-zAz][a-zA-Z0-9_-]+|'[^'\n\r]+')/, token: "variable-2"},
                        {regex: /(:?\.[a-zAz][a-zA-Z0-9_-]+|'[^'\n\r]+')/, token: "variable-3"},

                        {regex: /true|false|null/, token: "atom"},
                        {regex: /[0-9]+\.[0-9]+[eE]?[+-]?[0-9]+/, token: "number"},
                        {regex: /[0-9]+\.[0-9]+/, token: "number"},
                        {regex: /[0-9]+/, token: "number"},

                        {regex: /\[/, next: "join"},
                        {regex: /\{/, indent: true, next: "block"},
                    ],

                    join: [
                        {regex: /inner|outer|cross|on/, token: "keyword"},
                        {regex: /(:?[a-zAz][a-zA-Z0-9_-]+|'[^'\n\r]+')/, token: "variable-2"},
                        {regex: /]/, next: "start"},

                    ],

                    block: [
                        {regex: /rename|fold|unfold|keep|drop|filter|to/, token: "keyword"},
                        {regex: /(:?[a-zAz][a-zA-Z0-9_-]+|'[^'\n\r]+')/, token: "variable-2"},
                        {regex: /(:?\.[a-zAz][a-zA-Z0-9_-]+|'[^'\n\r]+')/, token: "variable-3"},

                        {regex: /true|false|null/, token: "atom"},
                        {regex: /[0-9]+\.[0-9]+[eE]?[+-]?[0-9]+/, token: "number"},
                        {regex: /[0-9]+\.[0-9]+/, token: "number"},
                        {regex: /[0-9]+/, token: "number"},

                        {regex: /}/, dedent: true, next: "start"}
                    ],

                    // The multi-line comment state.
                    comment: [
                        {regex: /.*?\*\//, dedent: true, token: "comment", next: "start"},
                        {regex: /.*/, token: "comment"}
                    ],
                    // The meta property contains global information about the mode. It
                    // can contain properties like lineComment, which are supported by
                    // all modes, and also directives like dontIndentStates, which are
                    // specific to simple modes.
                    meta: {
                        dontIndentStates: ["comment"],
                        //lineComment: "//"
                    }
                });
                window.CodeMirror = codemirror;
                return this;
            }
        }
    },
    map: {
        'codemirror-simple': {
            '../../lib/codemirror': 'codemirror'
        }
    }
});

require([
    'rd', 'angular', 'codemirror-simple',
    'smart-table', 'ui.bootstrap', 'ui.codemirror',
    '../../js/directives/vtl-codemirror',
    '../../js/directives/vtl-dataset',
    '../../js/directives/vtl-data',
    '../../js/directives/vtl-example',
    '../../js/directives/vtl-table'], function (rd, angular, sm) {

    rd.Diagram.VERTICAL_SEPARATION = 20;
    rd.Diagram.INTERNAL_ALIGNMENT = "left";
    rd.Diagram(
        rd.Stack(
            rd.Sequence(
                rd.Terminal('FOLD'),
                rd.ZeroOrMore(
                    rd.NonTerminal('componentReference'),
                    rd.Terminal(',')
                )
            ),
            rd.Sequence(
                rd.Terminal('TO'), rd.ZeroOrMore(rd.NonTerminal('identifier'), rd.Terminal(',')
                )
            )
        )
    ).addTo(document.getElementById("foldClause"));

    angular.module('documentation', ['smart-table', 'ui.bootstrap', 'ui.codemirror',
        'vtl.code', 'vtl.data', 'vtl.dataset', 'vtl.table', 'vtl']);
    angular.bootstrap(angular.element('#content')[0], ['documentation']);

});

// Gave up.
//
// require(['react', 'react-dom', 'react-codemirror'], function (React, ReactDOM, CodeMirror) {
//     console.log(React);
//     console.log(ReactDOM);
//
//     // now you can render your React elements
//     ReactDOM.render(
//         React.createElement('p', {}, 'Hello, AMD!'),
//         document.getElementById('root')
//     );
// });
