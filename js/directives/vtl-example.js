/*global define*/
'use strict';

define(['angular',], function (angular) {
    var moduleName = 'vtl';
    angular.module(moduleName, [])
        .directive('vtlExample', function () {
            return {
                restrict: 'AE',
                scope: {},
                transclude: true,
                template: '<div class="vtl-example"></div>',
                controller: ['$scope', '$http', function ($scope, $http) {

                    $scope.outputDatasets = [];
                    $scope.inputDatasets = [];
                    $scope.$watch('outputDatasets', function (newValues, oldValues) {
                        // Copy
                        $scope.datasets = $scope.inputDatasets.slice();
                        if (newValues) {
                            var seen = {};
                            var output = newValues, input = $scope.datasets;
                            for (var i = 0; i < input.length; i++) {
                                seen[input[i].name] = true;
                            }
                            for (var i = 0; i < output.length; i++) {
                                if (!seen[output[i].name]) {
                                    $scope.datasets.push(output[i]);
                                }
                            }
                        }
                    });

                    $scope.errors = null;
                    $scope.loading = false;

                    this.execute = function (expression) {

                        $scope.loading = true;
                        $scope.errors = null;

                        var execution = {
                            datasets: $scope.inputDatasets,
                            expression: expression
                        };

                        // Simple GET request example:
                        $http({
                            data: execution,
                            method: 'POST',
                            url: 'https://api.hadrien.io/vtl/execute2'
                        }).then(function successCallback(response) {
                            $scope.loading = false;
                            $scope.outputDatasets = response.data.datasets;
                        }, function failureCallback(response) {
                            $scope.loading = false;
                            $scope.errors = response.data;
                        });
                    };

                    this.addInput = function (dataset) {
                        $scope.inputDatasets.push(dataset);
                    };

                }],
                link: function (scope, element, attrs, ctrl, transclude) {
                    // Use the function to bind the transcluded content to
                    // isolated scope.
                    transclude(scope, function (clone, scope) {
                        element.find('div').append(clone);
                    });
                }
            };
        });
    return moduleName;
});
