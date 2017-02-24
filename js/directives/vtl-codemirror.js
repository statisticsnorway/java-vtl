/*global define*/
'use strict';

define(['angular', 'ui.codemirror'], function (angular) {
    var moduleName = 'vtl.code';
    angular.module(moduleName, ['ui.codemirror'])
        .directive('vtlCode', function () {
            return {
                require: '^^vtlExample',
                restrict: 'E',
                transclude: true,
                //replace: true,
                templateUrl: '../../js/directives/vtl-codemirror.html',
                link: function preLink(scope, iElement, iAttrs, controller, transclude) {
                    var expression = transclude().text();
                    scope.expression = expression.replace(/^\s+|\s+$/g, '').trim();
                    scope.execute = controller.execute;
                }
            };
        });
    return moduleName;
});
