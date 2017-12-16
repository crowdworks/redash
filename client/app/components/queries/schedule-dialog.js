import moment from 'moment';
import { map, range, partial } from 'underscore';

import template from './schedule-dialog.html';

function padWithZeros(size, v) {
  let str = String(v);
  if (str.length < size) {
    str = `0${str}`;
  }
  return str;
}

function queryTimePicker() {
  return {
    restrict: 'E',
    scope: {
      refreshType: '=',
      query: '=',
      saveQuery: '=',
    },
    template: `
      <select ng-disabled="refreshType != 'daily'" ng-model="hour" ng-change="updateSchedule()" ng-options="c as c for c in hourOptions"></select> :
      <select ng-disabled="refreshType != 'daily'" ng-model="minute" ng-change="updateSchedule()" ng-options="c as c for c in minuteOptions"></select>
    `,
    link($scope) {
      $scope.hourOptions = map(range(0, 24), partial(padWithZeros, 2));
      $scope.minuteOptions = map(range(0, 60, 5), partial(padWithZeros, 2));

      if ($scope.query.hasDailySchedule()) {
        const parts = $scope.query.scheduleInLocalTime().split(':');
        $scope.minute = parts[1];
        $scope.hour = parts[0];
      } else {
        $scope.minute = '15';
        $scope.hour = '00';
      }

      $scope.updateSchedule = () => {
        const newSchedule = moment().hour($scope.hour)
          .minute($scope.minute)
          .utc()
          .format('HH:mm');

        if (newSchedule !== $scope.query.schedule) {
          $scope.query.schedule = newSchedule;
          $scope.saveQuery();
        }
      };

      $scope.$watch('refreshType', () => {
        if ($scope.refreshType === 'daily') {
          $scope.updateSchedule();
        }
      });
    },
  };
}

function queryRefreshSelect() {
  return {
    restrict: 'E',
    scope: {
      refreshType: '=',
      query: '=',
      saveQuery: '=',
    },
    template: `<select
                ng-disabled="refreshType != 'periodic'"
                ng-model="query.schedule"
                ng-change="saveQuery()"
                ng-options="c.value as c.name for c in refreshOptions">
                <option value="">No Refresh</option>
                </select>`,
    link($scope) {
      $scope.refreshOptions = [
        {
          value: '60',
          name: 'Every minute',
        },
      ];

      [5, 10, 15, 30].forEach((i) => {
        $scope.refreshOptions.push({
          value: String(i * 60),
          name: `Every ${i} minutes`,
        });
      });

      range(1, 13).forEach((i) => {
        $scope.refreshOptions.push({
          value: String(i * 3600),
          name: `Every ${i}h`,
        });
      });

      $scope.refreshOptions.push({
        value: String(24 * 3600),
        name: 'Every 24h',
      });
      $scope.refreshOptions.push({
        value: String(7 * 24 * 3600),
        name: 'Every 7 days',
      });
      $scope.refreshOptions.push({
        value: String(14 * 24 * 3600),
        name: 'Every 14 days',
      });
      $scope.refreshOptions.push({
        value: String(30 * 24 * 3600),
        name: 'Every 30 days',
      });

      $scope.$watch('refreshType', () => {
        if ($scope.refreshType === 'periodic') {
          if ($scope.query.hasDailySchedule()) {
            $scope.query.schedule = null;
            $scope.saveQuery();
          }
        }
      });
    },

  };
}

function spreadsheetExportSettings() {
  return {
    restrict: 'E',
    scope: {
      query: '=',
      saveQuery: '=',
    },
    template: `
      <label for="spreadsheet-url">Spreadsheet URL:</label>
      <a href="/_welcome/spreadsheet_export.html" target="_blank">この機能について</a>
      <input type="text" ng-model="spreadsheetUrl" name="spreadsheet-url" style="width: 100%;">
      <button class="btn btn-default btn-s" ng-click="save()"><span class="fa fa-floppy-o"></span>Save</button>
      <button class="btn btn-default btn-s" ng-click="open()"><span class="fa fa-external-link"></span>Open</button>
    `,
    link($scope) {
      if ($scope.query && $scope.query.options) {
        $scope.spreadsheetUrl = $scope.query.options.spreadsheetUrl;
      } else {
        $scope.spreadsheetUrl = '';
      }

      $scope.save = () => {
        if (!$scope.query.options) {
          $scope.query.options = {};
        }
        $scope.query.options.spreadsheetUrl = $scope.spreadsheetUrl;
        $scope.saveQuery();
      };

      $scope.open = () => {
        if ($scope.spreadsheetUrl && $scope.spreadsheetUrl.startsWith('https://')) {
          window.open($scope.spreadsheetUrl);
        }
      };
    },
  };
}

const ScheduleForm = {
  controller() {
    this.query = this.resolve.query;
    this.saveQuery = this.resolve.saveQuery;

    if (this.query.hasDailySchedule()) {
      this.refreshType = 'daily';
    } else {
      this.refreshType = 'periodic';
    }

    this.close = this.resolve.close;
  },
  bindings: {
    resolve: '<',
    close: '&',
    dismiss: '&',
  },
  template,
};

export default function init(ngModule) {
  ngModule.directive('queryTimePicker', queryTimePicker);
  ngModule.directive('queryRefreshSelect', queryRefreshSelect);
  ngModule.directive('spreadsheetExportSettings', spreadsheetExportSettings);
  ngModule.component('scheduleDialog', ScheduleForm);
}
