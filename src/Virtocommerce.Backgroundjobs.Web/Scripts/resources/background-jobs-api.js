angular.module('Virtocommerce.Backgroundjobs')
    .factory('Virtocommerce.Backgroundjobs.webApi', ['$resource', function ($resource) {
        return $resource('api/background-jobs');
    }]);
