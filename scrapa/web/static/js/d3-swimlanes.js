/*
The MIT License (MIT)

Copyright (c) 2013 bill@bunkat.com

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

/* jshint quotmark: true */
/* globals d3 */


function swimlaneChart(config) {
  'use strict';

  var width = 960;
  var height = 500;
  var maxTime = 1000 * 60;
  config = config || {};
  var margin = config.margin || {top: 20, right: 15, bottom: 15, left: 60};

  function my(selection){

    selection.each(function(data, i){

      width = width - margin.right - margin.left;
      height = height - margin.top - margin.bottom;

      var chart = d3.select(this)
        .append('svg:svg')
        .attr('width', width + margin.right + margin.left)
        .attr('height', height + margin.top + margin.bottom)
        .attr('class', 'chart');

      chart.append('defs').append('clipPath')
        .attr('id', 'clip')
        .append('rect')
          .classed('defs-rect', true)
          .attr('width', width);

      var main = chart.append('g')
        .classed('main', true)
        .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
        .attr('width', width)
        .attr('class', 'main');

      main.append('g')
        .classed('main-group-lanelines', true);
      main.append('g')
        .classed('main-group-lanetext', true);

      var mini = chart.append('g')
        .classed('mini', true)
        .attr('width', width)
        .attr('class', 'mini');

      mini.append('g')
        .classed('mini-group-lanelines', true);
      mini.append('g')
        .classed('mini-group-lanetext', true);

      main.append('g')
        .attr('class', 'main axis date');

      main.append('g')
        .attr('transform', 'translate(0,0.5)')
        .attr('class', 'main axis month');

      main.append('line')
        .attr('y1', 0)
        .attr('class', 'main todayLine')
        .attr('clip-path', 'url(#clip)');

      mini.append('line')
        .attr('y1', 0)
        .attr('class', 'todayLine');

      mini.append('g')
        .attr('class', 'mini-item-group');

      mini.append('rect')
        .attr('class', 'mini-item-rect')
        .attr('pointer-events', 'painted')
        .attr('visibility', 'hidden')
        .attr('width', width);

      my.brush = d3.svg.brush();

      mini.append('g')
        .attr('class', 'x brush');

      main.append('g')
        .attr('class', 'main-item-rects')
        .attr('clip-path', 'url(#clip)');

      mini.append('g')
        .attr('class', 'axis date');

      mini.append('g')
        .attr('transform', 'translate(0,0.5)')
        .attr('class', 'axis month');

    });
  }

  my.update = function(selection) {

    selection.each(function(data, i){
      var now = new Date();

      // generates a single path for each item class in the mini display
      // ugly - but draws mini 2x faster than append lines or line generator
      // is there a better way to do a bunch of lines as a single path with d3?
      function getPaths(items) {
        var paths = {}, d, offset = 0.5 * y2(1) + 0.5, result = [];
        for (var i = 0; i < items.length; i++) {
          d = items[i];
          if (!paths[d.class]) {
            paths[d.class] = '';
          }
          paths[d.class] += ['M',x(d.start),(y2(d.lane) + offset),'H',x(d.end)].join(' ');
        }

        for (var className in paths) {
          result.push({class: className, path: paths[className]});
        }

        return result;
      }

      function drawRequests(){
        var itemRects = main.select('.main-item-rects');

        var visItems, minExtent, maxExtent;

        if (my.brush.empty()){
          visItems = items;
          minExtent = x.domain()[0];
          maxExtent = x.domain()[1];
        } else {
          minExtent = my.brush.extent()[0];
          maxExtent = my.brush.extent()[1];
          visItems = items.filter(function (d) {
            return d.start < maxExtent && d.end > minExtent;
          });
        }

        x1.domain(my.brush.empty() ? x.domain() : my.brush.extent());

        //x1Offset.range([0, x1(d3.time.day.ceil(now) - x1(d3.time.day.floor(now)))]);

        // shift the today line
        main.select('.main.todayLine')
          .attr('x1', x1(now) + 0.5)
          .attr('x2', x1(now) + 0.5);

        // update the axis
        main.select('.main.axis.date').call(x1DateAxis);
        main.select('.main.axis.month').call(x1MonthAxis)
          .selectAll('text')
            .attr('dx', 5)
            .attr('dy', 12);

        // upate the item rects
        var rects = itemRects.selectAll('rect')
          .data(visItems, function (d) { return d.id; });

        rects
          .enter().append('rect')
            .attr('class', function(d) { return 'mainItem ' + d.class; });

        rects
          .attr('x', function(d) { return x1(d.start); })
          .attr('y', function(d) { return y1(d.lane) + 0.1 * y1(1) + 0.5; })
          .attr('width', function(d) { return x1(d.end) - x1(d.start); })
          .attr('height', function(d) { return 0.8 * y1(1); })
          .on('click', function(d){
            if (d.data) {
              window.open('data:text/html;charset=utf-8;base64,' + d.data, 'request');
            }
          });

        rects.exit().remove();
      }


      var lanes = data.lanes;
      var items = data.items;

      var miniHeight = lanes.length * 12 + 50;
      var mainHeight = height - miniHeight - 50;

      var ext = d3.extent(lanes, function(d) { return d.id; });
      var y1 = d3.scale.linear().domain([ext[0], ext[1] + 1]).range([0, mainHeight]);
      var y2 = d3.scale.linear().domain([ext[0], ext[1] + 1]).range([0, miniHeight]);

      var min = d3.time.minute(d3.min(items, function(d) { return d.start; }));
      if (maxTime !== undefined) {
        // Use minimum of maxTime and current maximum time needed
        min = new Date(Math.max(min.getTime(), (new Date(now.getTime() - maxTime)).getTime()));
      }

      var x = d3.time.scale()
        .domain([min, now])
        .range([0, width]);
      var x1 = d3.time.scale().range([0, width]);

      var chart = d3.select(this);

      var main = chart.select('.main');
      main.attr('height', mainHeight);

      var mini = chart.select('.mini');
      mini.attr('height', miniHeight)
        .attr('transform', 'translate(' + margin.left + ',' + (mainHeight + 60) + ')');

      chart.select('.defs-rect')
        .attr('height', mainHeight);

      var xDateAxis = d3.svg.axis()
        .scale(x)
        .orient('bottom')
        .ticks(d3.time.minute, (x.domain()[1] - x.domain()[0]) > 15552e6 ? 2 : 1)
        .tickFormat(d3.time.format('%d'))
        .tickSize(6, 0, 0);

      var x1DateAxis = d3.svg.axis()
        .scale(x1)
        .orient('bottom')
        .ticks(d3.time.second, 1)
        .tickFormat(d3.time.format('%S.%L'))
        .tickSize(6, 0, 0);

      var xMonthAxis = d3.svg.axis()
        .scale(x)
        .orient('top')
        .ticks(d3.time.minute, 1)
        .tickFormat(d3.time.format('%H %M.%S'))
        .tickSize(15, 0, 0);

      var x1MonthAxis = d3.svg.axis()
        .scale(x1)
        .orient('top')
        .ticks(d3.time.minute, 1)
        .tickFormat(d3.time.format('%M:%S'))
        .tickSize(15, 0, 0);

      x1DateAxis.ticks(d3.time.seconds, 4).tickFormat(d3.time.format('%S %L'));
      x1MonthAxis.ticks(d3.time.minutes, 1).tickFormat(d3.time.format('%M:%S'));


      // draw the lanes for the main chart
      var laneLines = main.select('.main-group-lanelines').selectAll('.laneLines')
        .data(lanes, function (d) { return d.id; });

      laneLines
        .enter().append('line')
          .classed('laneLines', true);

      laneLines
        .attr('x1', 0)
        .attr('y1', function(d) { return d3.round(y1(d.id)) + 0.5; })
        .attr('x2', width)
        .attr('y2', function(d) { return d3.round(y1(d.id)) + 0.5; })
        .attr('stroke', function(d) { return d.label === '' ? 'white' : 'lightgray'; });
      laneLines.exit().remove();

      var laneText = main.select('.main-group-lanetext').selectAll('.laneText')
        .data(lanes, function (d) { return d.id; });

      laneText
        .enter().append('text')
        .text(function(d) { return d.label; })
        .attr('x', -10)
        .attr('dy', '0.5ex')
        .attr('text-anchor', 'end')
        .attr('class', 'laneText');

      laneText
        .attr('y', function(d) { return y1(d.id + 0.5); });

      laneText.exit().remove();

      // draw the lanes for the mini chart
      var miniLaneLines = mini.select('.mini-group-lanelines')
        .selectAll('.laneLines')
        .data(lanes, function (d) { return d.id; });

      miniLaneLines
        .enter().append('line')
        .classed('laneLines', true)
        .attr('x1', 0)
        .attr('x2', width);

      miniLaneLines
        .attr('y1', function(d) { return d3.round(y2(d.id)) + 0.5; })
        .attr('y2', function(d) { return d3.round(y2(d.id)) + 0.5; })
        .attr('stroke', function(d) { return d.label === '' ? 'white' : 'lightgray'; });
      miniLaneLines.exit().remove();

      var miniLaneText = mini.select('.mini-group-lanetext')
        .selectAll('.laneText')
        .data(lanes, function (d) { return d.id; });

      miniLaneText
        .enter()
          .append('text')
          .attr('x', -10)
          .attr('dy', '0.5ex')
          .attr('text-anchor', 'end')
          .attr('class', 'laneText');

      miniLaneText
        .text(function(d) { return d.label; })
        .attr('y', function(d) { return y2(d.id + 0.5); });

      miniLaneText.exit().remove();

      main.select('.main.axis.date')
        .attr('transform', 'translate(0,' + mainHeight + ')');
      mini.select('.axis.date')
        .attr('transform', 'translate(0,' + miniHeight + ')')
        .call(xDateAxis);

      mini.select('.axis.month')
        .call(xMonthAxis)
        .selectAll('text')
          .attr('dx', 5)
          .attr('dy', 12);

      // draw a line representing today's date
      main.select('.main.todayLine')
        .attr('y2', mainHeight);

      mini.select('.todayLine')
        .attr('x1', x(now) + 0.5)
        .attr('x2', x(now) + 0.5)
        .attr('y2', miniHeight);

      var miniItems = mini.select('.mini-item-group').selectAll('.miniItem')
        .data(getPaths(items), function(d){ return d.class; });

      miniItems
        .enter().append('path')
        .attr('class', function(d) { return 'miniItem ' + d.class; });

      miniItems
        .attr('d', function(d) { return d.path; });

      // invisible hit area to move around the selection window
      mini.select('.mini-item-rect')
        .attr('height', miniHeight);
      //
      my.brush
        .x(x)
        .on('brush', null)
        .on('brush', drawRequests);

      mini.select('.x.brush')
        .call(my.brush)
        .selectAll('rect')
          .attr('y', 1)
          .attr('height', miniHeight - 1);

      drawRequests();
    });
  };

  my.width = function(value) {
    if (!arguments.length) return width;
    width = value;
    return my;
  };

  my.height = function(value) {
    if (!arguments.length) return height;
    height = value;
    return my;
  };

  my.maxTime = function(value) {
    if (!arguments.length) return maxTime;
    maxTime = value;
    return my;
  };

  return my;
}
