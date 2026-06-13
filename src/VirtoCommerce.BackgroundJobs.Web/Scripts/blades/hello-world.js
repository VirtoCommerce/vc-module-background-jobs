angular.module('VirtoCommerce.BackgroundJobs')
    .controller('VirtoCommerce.BackgroundJobs.helloWorldController', ['$scope', 'VirtoCommerce.BackgroundJobs.webApi', function ($scope, api) {
        var blade = $scope.blade;
        blade.title = 'BackgroundJobs';

        blade.refresh = function () {
            api.get(function (data) {
                blade.title = 'background-jobs.blades.hello-world.title';
                blade.data = data.result;
                blade.isLoading = false;
            });
        };

        blade.refresh();
    }]);
