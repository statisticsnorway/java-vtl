/*global define*/
'use strict';
define(['angular', 'require', 'ui.codemirror'], function (angular, req) {
    var moduleName = 'vtl.code';
    angular.module(moduleName, ['ui.codemirror'])
        .directive('vtlCode', function () {
            return {
                require: '^^vtlExample',
                restrict: 'E',
                transclude: true,
                //replace: true,
                templateUrl: req.toUrl('./vtl-codemirror.html'),
                link: function preLink(scope, iElement, iAttrs, controller, transclude) {
                    var expression = transclude().text();
                    scope.expression = expression.replace(/^\s+|\s+$/g, '').trim();
                    scope.execute = controller.execute;
                }
            };
        });
    return moduleName;
});
