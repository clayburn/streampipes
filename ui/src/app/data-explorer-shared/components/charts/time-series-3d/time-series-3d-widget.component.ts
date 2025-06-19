/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import { Component, inject, OnDestroy, OnInit } from '@angular/core';
import {
    DataExplorerField,
    SpQueryResult,
} from '@streampipes/platform-services';
import { BaseDataExplorerWidgetDirective } from '../base/base-data-explorer-widget.directive';
import { ECharts } from 'echarts/core';
import { EChartsOption } from 'echarts';
import { Subject, Subscription } from 'rxjs';
import { ResizeEchartsService } from '../../../services/resize-echarts.service';
import { debounceTime } from 'rxjs/operators';
import { TimeSeries3dWidgetModel } from './model/time-series-3d-widget.model';
import { EchartsBasicOptionsGeneratorService } from '../../../echarts-renderer/echarts-basic-options-generator.service';
import { WidgetEchartsAppearanceConfig } from '../../../models/dataview-dashboard.model';

@Component({
    selector: 'sp-data-explorer-time-series-3d-widget',
    templateUrl: '../base/echarts-widget.component.html',
    standalone: false,
})
export class TimeSeries3dWidgetComponent
    extends BaseDataExplorerWidgetDirective<TimeSeries3dWidgetModel>
    implements OnInit, OnDestroy
{
    eChartsInstance: ECharts;
    currentWidth: number;
    currentHeight: number;

    option: EChartsOption;

    configReady = false;
    latestData: SpQueryResult[];

    renderSubject = new Subject<void>();
    renderSubject$: Subscription;
    resizeEcharts$: Subscription;

    private resizeEchartsService = inject(ResizeEchartsService);
    private echartsBasicOptionsGeneratorService = inject(
        EchartsBasicOptionsGeneratorService,
    );

    widgetTypeLabel: string;

    ngOnInit(): void {
        super.ngOnInit();
        this.resizeEcharts$ =
            this.resizeEchartsService.echartsResizeSubject.subscribe(width => {
                this.currentWidth = width - this.widthOffset;
                this.applySize(this.currentWidth, this.currentHeight);
                this.refreshView();
            });
        this.renderSubject$ = this.renderSubject
            .pipe(debounceTime(300))
            .subscribe(() => {
                this.renderChartOptions(this.latestData);
            });
        this.widgetTypeLabel = this.widgetRegistryService.getChartTemplate(
            this.dataExplorerWidget.widgetType,
        ).label;
    }

    beforeDataFetched() {}

    onDataReceived(spQueryResult: SpQueryResult[]) {
        this.renderChartOptions(spQueryResult);
        this.latestData = spQueryResult;
        this.setShownComponents(false, true, false, false);
    }

    onResize(width: number, height: number) {
        this.currentWidth = width;
        this.currentHeight = height;
        this.configReady = true;
        this.applySize(width, height);
        if (this.latestData) {
            this.renderSubject.next();
        }
    }

    onChartInit(ec: ECharts) {
        this.eChartsInstance = ec;
        this.applySize(this.currentWidth, this.currentHeight);
    }

    applySize(width: number, height: number) {
        if (this.eChartsInstance) {
            this.eChartsInstance.resize({ width, height });
        }
    }

    renderChartOptions(spQueryResult: SpQueryResult[]): void {
        const chartData = this.convertChartData(spQueryResult);
        if (
            this.dataExplorerWidget.visualizationConfig.configurationValid ===
                undefined ||
            this.dataExplorerWidget.visualizationConfig.configurationValid ===
                true
        ) {
            this.showInvalidConfiguration = false;
            this.option = {
                tooltip: {},
                visualMap: {
                    show: true,
                    min: chartData.min,
                    max: chartData.max,
                    top: '0px',
                    right: '50px',
                    orient: 'horizontal',
                    dimension: 2,
                    inRange: {
                        color: [
                            '#313695',
                            '#4575b4',
                            '#74add1',
                            '#abd9e9',
                            '#e0f3f8',
                            '#ffffbf',
                            '#fee090',
                            '#fdae61',
                            '#f46d43',
                            '#d73027',
                            '#a50026',
                        ],
                    },
                },
                xAxis3D: {
                    type: 'time',
                    name: 'Time',
                    axisLabel: {
                        formatter: v => {
                            return new Date(v).toLocaleString();
                        },
                    },
                },
                yAxis3D: {
                    type: 'category',
                    name: 'Category',
                    data: chartData.yAxisLabels,
                },
                zAxis3D: {
                    type: 'value',
                    name: this.dataExplorerWidget.visualizationConfig
                        .selectedProperty.runtimeName,
                },
                grid3D: {
                    viewControl: {
                        projection: 'perspective',
                    },
                },
                series: [
                    {
                        type: this.dataExplorerWidget.visualizationConfig
                            .chartType,
                        symbolSize:
                            this.dataExplorerWidget.visualizationConfig
                                .symbolSize,
                        wireframe: {
                            show: false,
                        },
                        data: chartData.chartData,
                    } as any,
                ],
            };
            const baseConfig =
                this.echartsBasicOptionsGeneratorService.makeBaseConfig(
                    this.dataExplorerWidget
                        .baseAppearanceConfig as WidgetEchartsAppearanceConfig,
                    {},
                );
            this.option = Object.assign(this.option, baseConfig);
        } else {
            this.showInvalidConfiguration = true;
        }
    }

    refreshView() {
        this.renderSubject.next();
    }

    handleUpdatedFields(
        addedFields: DataExplorerField[],
        removedFields: DataExplorerField[],
    ) {}

    convertChartData(input: any[]) {
        const allSeries = input?.[0]?.allDataSeries ?? [];
        const yAxisLabels: string[] = [];
        const chartData: [number, number, number][] = [];

        let min = Number.POSITIVE_INFINITY;
        let max = Number.NEGATIVE_INFINITY;

        allSeries.forEach((series, sensorIdx) => {
            const tagString = Object.entries(series.tags || {})
                .map(([k, v]) => `${k}=${v}`)
                .join(', ');
            yAxisLabels.push(tagString || `Sensor ${sensorIdx + 1}`);

            series.rows.forEach(row => {
                const [timestamp, distance] = row;
                chartData.push([timestamp, sensorIdx, distance]);
                if (distance < min) min = distance;
                if (distance > max) max = distance;
            });
        });

        return {
            chartData,
            yAxisLabels,
            min,
            max,
        };
    }

    ngOnDestroy(): void {
        this.cleanupSubscriptions();
        this.resizeEcharts$?.unsubscribe();
        this.renderSubject$?.unsubscribe();
    }
}
