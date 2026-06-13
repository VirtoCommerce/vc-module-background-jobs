// Call this to register your module to main application
var moduleName = 'Virtocommerce.Backgroundjobs';

if (AppDependencies !== undefined) {
    AppDependencies.push(moduleName);
}

angular.module(moduleName, [])
    .config(['$stateProvider',
        function ($stateProvider) {
            $stateProvider
                .state('workspace.BackgroundjobsState', {
                    url: '/background-jobs',
                    templateUrl: '$(Platform)/Scripts/common/templates/home.tpl.html',
                    controller: [
                        'platformWebApp.bladeNavigationService',
                        function (bladeNavigationService) {
                            var newBlade = {
                                id: 'blade1',
                                controller: 'Virtocommerce.Backgroundjobs.helloWorldController',
                                template: 'Modules/$(Virtocommerce.Backgroundjobs)/Scripts/blades/hello-world.html',
                                isClosingDisabled: true,
                            };
                            bladeNavigationService.showBlade(newBlade);
                        }
                    ]
                });
        }
    ])
    .run(['platformWebApp.mainMenuService', '$state',
        function (mainMenuService, $state) {
            //Register module in main menu
            var menuItem = {
                path: 'browse/background-jobs',
                icon: 'fa fa-cube',
                title: 'BackgroundJobs',
                priority: 100,
                action: function () { $state.go('workspace.BackgroundjobsState'); },
                permission: 'background-jobs:access',
            };
            mainMenuService.addMenuItem(menuItem);
        }
    ]);
