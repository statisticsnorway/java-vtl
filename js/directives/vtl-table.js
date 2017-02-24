/*global define*/
'use strict';

define(['angular',], function (angular) {
    var moduleName = 'vtl.table';
    angular.module(moduleName, [])
        .directive('vtlTable', function () {
            return {
                restrict: 'E',
                scope: {
                    dataset: '=',
                },
                replace: true,
                templateUrl: '../../js/directives/vtl-table.html',
            };
        });
    return moduleName;
});
