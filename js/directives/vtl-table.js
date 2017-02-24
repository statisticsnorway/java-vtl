/*global define*/
'use strict';

define(['angular','require'], function (angular, req) {
    var moduleName = 'vtl.table';
    angular.module(moduleName, [])
        .directive('vtlTable', function () {
            return {
                restrict: 'E',
                scope: {
                    dataset: '=',
                },
                replace: true,
                templateUrl: req.toUrl('./vtl-table.html'),
            };
        });
    return moduleName;
});
