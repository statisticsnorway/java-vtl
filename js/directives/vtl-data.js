/*global define*/
'use strict';

define(['angular', 'require'], function (angular, req) {
    var moduleName = 'vtl.data';
    angular.module(moduleName, [])
        .directive('vtlData', function () {
            return {
                restrict: 'E',
                scope: {
                    datasets: '=',
                    errors: '='
                },
                templateUrl: req.toUrl('./vtl-data.html')
            };

        });

    return moduleName;
});
