angular.module('VirtoCommerce.BackgroundJobs')
    .factory('VirtoCommerce.BackgroundJobs.webApi', ['$resource', function ($resource) {
        return $resource('api/background-jobs');
    }]);
