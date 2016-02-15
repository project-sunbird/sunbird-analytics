var app = angular.module('playerApp', ['sunburst.services', 'sunburst.directives']);
var graphData = {};
var graphDataFormated = {};
var graphLoadTimeout;
var learnProfData;

var jsContants = {
    SERVER_PATH: 'http://lp-sandbox.ekstep.org:8080/taxonomy-service/',
    API: {
        LITERACY: 'v2/domain/graph/literacy',
        NUMERACY: 'v2/domain/graph/numeracy'
    },
    GRAPH_SUNBURST: 'sunburstGraph',
    GRAPH_TREE: 'treeGraph',
    GRAPH_SIGMA: 'sigmaGraph',
    DEFAULT_COLOR: '#000000',
    NODE_COLORS: ['#000000', '#3182BD', '#31A354'],
    FORCE_LAYOUT_STOP_TIMER: 15000
}

app.controller('LearningMapController', ['$scope', '$timeout', '$http', '$rootScope', function($scope, $timeout, $http, $rootScope) {
    $rootScope.sunburstLoaded = false;
    $rootScope.graphType = "";
    $scope.graphData = {};

    $scope.taxonomyObjects = [{
        id: 'concept',
        label: "Broad Concept"
    }, {
        id: 'subConcept',
        label: "Sub Concept"
    }, {
        id: 'microConcept',
        label: "Micro Concept"
    }];
    $scope.constants;

    $scope.init = function() {
        $scope.constants = jsContants;
        $scope.showGraph($scope.constants.GRAPH_SUNBURST, $scope.constants.API.LITERACY);
    }

    $scope.showGraphOfType = function(type) {
        $rootScope.graphType = type;
        loadGraph($rootScope.graphType, $scope);
    }

    $scope.showGraph = function(graphType, api) {
        if (!(_.isUndefined(graphType))) {
            $rootScope.graphType = graphType;

            if (!(_.isUndefined(api))) {
                $scope.getGraphData(api);
                //getDataFromAPI($scope, api);
            } else {
                loadGraph($rootScope.graphType, $scope);
            }
        } else {
            alet('Please specify graph type, ex: sunburstGraph');
        }
    }

    $scope.checkGraphType = function(type) {
        if (type.toString().toLowerCase() === $rootScope.graphType.toLowerCase()) {
            return true;
        } else {
            return false;
        }
    }

    $scope.getGraphData = function(api) {
        $http({
                method: 'GET',
                url: $scope.constants.SERVER_PATH + api,
                headers: {
                    'user-id': 'ekstep'
                }
            })
            .then(function(response) {
                graphData = response.data.result;
                console.log('graphData', graphData);
                var nodes = {},
                    rootNode = undefined,
                    allNodes = [];

                _.each(graphData.nodes, function(node) {
                    nodes[node.identifier] = node;
                    allNodes.push({
                        id: node.identifier,
                        name: node.metadata.name,
                        code: node.metadata.code
                    });
                    if (_.isEqual(node.objectType, 'Domain')) {
                        rootNode = node;
                    }
                });
                var graph = $scope.getNode(nodes, rootNode, 0);
                $scope.graphDataFormated = graph;
                graphDataFormated = graph;

                // Load graph 
                loadGraph($rootScope.graphType, $scope);
            });
    }

    $scope.getNode = function(nodes, graphNode, level) {
        var node = {
            name: graphNode.metadata.name,
            conceptId: graphNode.identifier,
            gamesCount: graphNode.metadata.gamesCount,
            children: [],
            level: level,
            size: 1,
            sum: 0,
            concepts: 0,
            subConcepts: 0,
            microConcepts: 0
        }
        var children = _.filter(graphNode.outRelations, function(relation) {
            return relation.relationType === 'isParentOf';
        });
        if (children && children.length > 0) {
            _.each(children, function(relation) {
                //console.log('relation', relation);
                var childNode = nodes[relation.endNodeId];
                //console.log('childNode', childNode);
                node.children.push($scope.getNode(nodes, childNode, (level + 1)));
            });
        }
        node.sum = node.children.length;
        switch (level) {
            case 0:
                node.concepts = node.sum;
                break;
            case 1:
                node.subConcepts = node.sum;
                break;
            case 2:
                node.microConcepts = node.sum;
                break;
        }
        node.children.forEach(function(childNode) {
            node.sum += childNode.sum;
            node.concepts += childNode.concepts;
            node.subConcepts += childNode.subConcepts;
            node.microConcepts += childNode.microConcepts;
        });
        return node;
    }
}]);

var $scope;

function getDataFromAPI(scope, api) {
    $scope = scope;
    /*$.ajax({method: 'GET', 
            url: 'http://lp-sandbox.ekstep.org:8080/taxonomy-service/'+ api, 
            headers:{'user-id': 'ekstep'}
        })*/
    $.get('json.data_literacy.json')
        .then(function(response) {
            graphData = response.result.subgraph;
            var nodes = {},
                rootNode = undefined,
                allNodes = [];

            _.each(graphData.nodes, function(node) {
                nodes[node.identifier] = node;
                allNodes.push({
                    id: node.identifier,
                    name: node.metadata.name,
                    code: node.metadata.code
                });
                if (_.isEqual(node.objectType, 'Taxonomy')) {
                    rootNode = node;
                }
            });
            var graph = getNode(nodes, rootNode, 0);
            $scope.graphDataFormated = graph;
            graphDataFormated = graph;

            // Load graph 
            loadGraph($scope.$root.graphType, $scope);
        });
}

function getNode(nodes, graphNode, level) {
    var node = {
        name: graphNode.metadata.name,
        conceptId: graphNode.identifier,
        gamesCount: graphNode.metadata.gamesCount,
        children: [],
        level: level,
        size: 1,
        sum: 0,
        concepts: 0,
        subConcepts: 0,
        microConcepts: 0
    }
    var children = _.filter(graphNode.outRelations, function(relation) {
        return relation.relationType === 'isParentOf';
    });
    if (children && children.length > 0) {
        _.each(children, function(relation) {
            //console.log('relation', relation);
            var childNode = nodes[relation.endNodeId];
            //console.log('childNode', childNode);
            node.children.push($scope.getNode(nodes, childNode, (level + 1)));
        });
    }
    node.sum = node.children.length;
    switch (level) {
        case 0:
            node.concepts = node.sum;
            break;
        case 1:
            node.subConcepts = node.sum;
            break;
        case 2:
            node.microConcepts = node.sum;
            break;
    }
    node.children.forEach(function(childNode) {
        node.sum += childNode.sum;
        node.concepts += childNode.concepts;
        node.subConcepts += childNode.subConcepts;
        node.microConcepts += childNode.microConcepts;
    });
    return node;
}

// Load graph based on graphType
function loadGraph(graphType, $scope) {
    $('#sunburst').empty();
    var graphType_lw = graphType.toString().toLowerCase();
    if (graphType_lw === $scope.constants.GRAPH_SUNBURST.toString().toLowerCase()) {
        loadSunburst($scope);
    } else if (graphType_lw === $scope.constants.GRAPH_TREE.toString().toLowerCase()) {
        graphLoadTimeout = setTimeout(function() {
            clearTimeout(graphLoadTimeout);
            loadTree($scope);
        }, 500);
    } else {
        graphLoadTimeout = setTimeout(function() {
            // Gave delay to show control bar using angularjs ng-show
            clearTimeout(graphLoadTimeout);
            loadSigmaGraph();
        }, 500);
    }
}

/**
================================== Sunburst ==================================
 */
function selectSunburstConcept(cid) {
    var nodes = d3.select("#sunburst").selectAll("#sunburst-path")[0];
    for (var i = 0; i < nodes.length; i++) {
        var node = nodes[i];
        var nodeCid = $(node).attr('cid');
        if (nodeCid == cid) {
            var event = document.createEvent("SVGEvents");
            event.initEvent("click", true, true);
            node.dispatchEvent(event);
        }
    }
}

/**
 * This is to load learner proficiency data
 */
function loadLearnerProfData() {
    $.get('json/learner_prof.json')
        .then(function(response) {
            learnProfData = response;
            //var learnerIds = _.keys(response);
        });
}

function updateProficiency() {
    var learnerDetails = learnProfData[$('#learnerId').val()];
    if (learnerDetails) {
        var concepts = _.keys(learnerDetails);

        //var nodes = s.graph.nodes();
        var paths = d3.selectAll("path")
            .transition()
            .duration(1000)
            .each("end", function() {
                var conceptID = this.attributes.cid.value;
                var found = false;
                _.each(concepts, function(con) {
                    if (con == conceptID) {
                        found = true;
                        return;
                    }
                });
                //var coceptIndex = _.indexOf(concepts, conceptID.toLowerCase());
                if (found) {
                    var conceptDetails = learnerDetails[conceptID];
                    var color = getProficiencyColor(conceptDetails.proficiency)
                    d3.select(this)
                        .style('fill', color)
                        .style('opacity', conceptDetails.precision / 100);
                    //.size = conceptDetails.precision/sigma.settings.maxNodeSize;
                    //nodeFound.color = getProficiencyColor(conceptDetails.proficiency);
                }
            });
    }
}

/**
 * Returns node color based on learner prodiciency
 */
function getProficiencyColor(proficiencyValue) {
    //proficiencyValue = Math.round(proficiencyValue);

    //default value == 1, because learner proficiency will be started from average value
    var profIndex;
    if (proficiencyValue < 0.3) {
        //Poor proficieny
        profIndex = 0;
    } else if (proficiencyValue > 0.7) {
        // High proficiency
        profIndex = 2;
    } else {
        //Average
        profIndex = 1;
    }

    return jsContants.NODE_COLORS[profIndex];
}


function loadSunburst($scope) {
    // Sunburst Code
    $scope.data;
    $scope.displayVis = false;
    $scope.color;
    $scope.contentList = [];
    // Browser onresize event
    window.onresize = function() {
        $scope.$apply();
    };

    // Traverses the data tree assigning a color to each node. This is important so colors are the
    // same in all visualizations
    $scope.assignColors = function(node) {
        $scope.getColor(node);
        _.each(node.children, function(c) {
            $scope.assignColors(c);
        });
    };
    // Calculates the color via alphabetical bins on the first letter. This will become more advanced.
    $scope.getColor = function(d) {
        d.color = $scope.color(d.name);
    };
    //$scope.color = ["#87CEEB", "#007FFF", "#72A0C1", "#318CE7", "#0000FF", "#0073CF"];
    $scope.color = d3.scale.ordinal().range(['#DDDDDD']); //"#33a02c", "#1f78b4", "#b2df8a", "#a6cee3", "#fb9a99", "#e31a1c", "#fdbf6f", "#ff7f00", "#6a3d9a", "#cab2d6", "#ffff99"]);

    if ($scope.graphDataFormated) {
        var root = angular.copy($scope.graphDataFormated);
        if (root) {
            $scope.assignColors(root);
            $scope.data = [root];
            $scope.conceptId = root.conceptId;
        }
    }

    // reset button
    $('#learnerSubmit').bind('click', function() {
        updateProficiency();
    });
    loadLearnerProfData();
}

/**
================================== Tree Graph ==================================
 */


function loadTree($scope) {
    return;
    var cid = $scope.sbConcept ? $scope.sbConcept.conceptId : null;
    showDNDTree($scope.graphDataFormated, 'treeLayout', {}, $scope, cid); // $scope.$parent.selectedTaxonomyId);

    $scope.selectConcept = function(conceptObj) {
        $scope.sbConcept = conceptObj;
        $scope.hoveredConcept = conceptObj;
    }
}
