var graphData = undefined;
var graphDataFormated = {};
var graphLoadTimeout;
var learnProfData = {};
var learnerDetails;
var currentGraph = undefined;
var recoEdges = [];

var jsContants = {
    API: {
        LITERACY: 'literacy',
        NUMERACY: 'numeracy'
    },
    GRAPH_SUNBURST: 'sunburstGraph',
    GRAPH_TREE: 'treeGraph',
    GRAPH_SIGMA: 'sigmaGraph',
    DEFAULT_COLOR: '#DDDDDD',
    NODE_COLORS: ['#000000', '#FF9000', '#DC5C05'],
    FORCE_LAYOUT_STOP_TIMER: 5000
}

function loadGraph(graphId) {
    $('#sigmaGraph').html('');
    if(graphData) {
        loadSigmaGraph();
    } else {
        $.ajax({
            method: 'GET',
            url: '/domain/map'
        })
        .then(function(response) {
            graphData = response;
            loadSigmaGraph();
        });
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

function resetNode(node) {
    node.color = color.hexToRgba(jsContants.DEFAULT_COLOR, 1);

    //Defining custom shapes for nodes
    var nodeSize = Math.random();
    if(node.objectType == 'Domain') {
        node.size = 1; // Default node size
    } else if(node.objectType == 'Dimension') {
        node.size = 0.12;
    } else {
        node.size = 0.12;
    }
}

var _learnerInfo = {};
function getLearnerInfo() {
    $($('#gradesDropdown option')[0]).attr('selected', true);
    filter.undo('grade').apply();
    var learnerId = $('#learnerId').val();
    deleteRecos();

    $.get('/learner/info/' + learnerId)
    .then(function(response) {
        $('#relevanceSubmit').show().removeClass('btn-selected');
        _learnerInfo = response;
        updateProficiency(response.proficiency);
        updateLearnerSnapshot(response.snapshot);
        //showRecommendations(response.recos);
    });
}

function updateProficiency(learnerDetails) {
    if (learnerDetails) {
        var concepts = _.keys(learnerDetails);

        var nodes = s.graph.nodes();
        _.each(nodes, function(node) {
            resetNode(node);
        });
        _.each(concepts, function(concept) {
            var nodeFound = _.findWhere(nodes, {
                'identifier': concept
            });
            if (nodeFound) {
                conceptDetails = learnerDetails[concept];
                var size = conceptDetails.precision/100;
                nodeFound.size = size > 1 ? 1 : size;
                nodeFound.color = getProficiencyColor(conceptDetails.proficiency);
                nodeFound.proficiency = conceptDetails.proficiency;
            }
        })
        s.refresh();
    }
}

function updateLearnerSnapshot(response) {
    $('#learnerSnapshotDetails').empty();
    if(response && response.n_of_sess_on_pf) {
        var tsa = [], mca = [];
        for (k in response.m_ts_on_an_act) {
            tsa.push(k + " : " + (Math.round(response.m_ts_on_an_act[k] * 100) / 100));
        }
        for (k in response.m_count_on_an_act) {
            mca.push(k + " : " + (Math.round(response.m_count_on_an_act[k] * 100) / 100));
        }
        var d = new Date(response.l_visit_ts);
        $('#learnerSnapshotDetails').append('<tr><td># of Sessions</td><td>' + response.n_of_sess_on_pf + '</td></tr>');
        $('#learnerSnapshotDetails').append('<tr><td>Total Timespent</td><td>' + response.t_ts_on_pf + ' sec </td></tr>');
        $('#learnerSnapshotDetails').append('<tr><td>Mean Timespent</td><td>' + Math.round(response.m_time_spent * 100) / 100 + ' sec </td></tr>');
        $('#learnerSnapshotDetails').append('<tr><td>Last Visit TS</td><td>' + d.toUTCString() + '</td></tr>');
        $('#learnerSnapshotDetails').append('<tr><td>Top K Content</td><td>' + response.top_k_content.join('<br/>') + '</td></tr>');
        $('#learnerSnapshotDetails').append('<tr><td>Mean TS on Activities</td><td>' + tsa.join("<br/>") + '</td></tr>');
        $('#learnerSnapshotDetails').append('<tr><td>Mean Count on Activities</td><td>' + mca.join("<br/>") + '</td></tr>');
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

function getNode(nodes, graphNode, level) {
    var node = {
        name: graphNode.name,
        conceptId: graphNode.identifier,
        gamesCount: graphNode.gamesCount,
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

/**
================================== Sigma Graph ==================================
 */


// Now that's the renderer has been implemented, let's generate a graph
// to render:
//
// filter: This is an example on how to use sigma filters plugin on a real-world graph.
//
var i,
    s,
    g = {
        nodes: [],
        edges: []
    },
    filter,
    filtersApplied = {
        objectType: [],
        relationType: [],
        nodeSelected: ""
    };

// Add a method to the graph model that returns an
// object with every neighbors of a node inside:
sigma.classes.graph.addMethod('neighbors', function(nodeId) {
    var k,
        neighbors = {},
        index = this.allNeighborsIndex[nodeId] || {};

    for (k in index)
        neighbors[k] = this.nodesIndex[k];

    return neighbors;
});

function loadSigmaGraph() {
    /**
     * This is an example on how to use sigma filters plugin on a real-world graph.
     */


    /**********************************************************
     *                                                        *
     *                    Node Drag                           *
     *                                                        *
     **********************************************************/
    function allowDrag(sig) {
        // Initialize the dragNodes plugin:
        var dragListener = sigma.plugins.dragNodes(sig, sig.renderers[0]);

        dragListener.bind('startdrag', function() {
            //console.log(event);
        });
        dragListener.bind('drag', function() {
            //console.log(event);
        });
        dragListener.bind('drop', function() {
            //console.log(event);
        });
        dragListener.bind('dragend', function() {
            //console.log(event);
        });
    }

    /**********************************************************
     *                                                        *
     *                    Neight Edges Hightlight             *
     *                                                        *
     **********************************************************/
    function selectNode(node) {
        s.stopForceAtlas2();
        //console.log(node);
        var toKeep = s.graph.neighbors(node.id);
        toKeep[node.id] = node;

        s.graph.edges().forEach(function(e) {
            if(e.type !== 'curvedArrow'){
                if (toKeep[e.source] && toKeep[e.target]) {
                    e.color = e.selectedColor;
                    e.size = 2;
                } else {
                    e.size = 1;
                    e.color = e.originalColor;
                }
            }
        });
        $('#json-viewer').jsonViewer(node);

        // Since the data has been modified, we need to
        // call the refresh method to make the colors
        // update effective.
        s.refresh();
    }

    function hightlightConnections() {
        // When a node is clicked, we check for each node
        // if it is a neighbor of the clicked one. If not,
        // we set its color as grey, and else, it takes its
        // original color.
        // We do the same for the edges, and we only keep
        // edges that have both extremities colored.
        s.bind('clickNode', function(e) {
            var node = e.data.node;
            selectNode(node);
        });

        s.bind('clickStage', function() {
            s.stopForceAtlas2();
            /*resetGraph();
            s.graph.edges().forEach(function(e) {
                e.color = e.originalColor;
            });
            s.graph.nodes().forEach(function(e) {
                e.size = sigma.settings.maxNodeSize / 2;
                e.color = jsContants.DEFAULT_COLOR;
            });
            s.refresh();*/
        });

        s.bind('clickEdge', function(evt) {
            //console.log(e.type, e.data.edge, e.data.captor);
            var shourceNodeId = evt.data.edge.source;
            var toKeep = s.graph.neighbors(shourceNodeId);
            toKeep[shourceNodeId] = _.find(s.graph.nodes(), {
                'id': shourceNodeId
            });

            s.graph.edges().forEach(function(e) {
                if (toKeep[e.source] && toKeep[e.target]) {
                    e.color = e.originalColor;
                    e.size = 3;
                } else {
                    e.size = 1;
                    e.color = '#eee';
                }
            });

            //$('#json-viewer').jsonViewer(evt.data.edge);

            // Since the data has been modified, we need to
            // call the refresh method to make the colors
            // update effective.
            s.refresh();
        });
    }

    /**********************************************************
     *                                                        *
     *                    JSON Formatter                      *
     *                                                        *
     **********************************************************/
    /*
     * Parsing input json data to the required sigma json format
     * http://www.jqueryscript.net/other/jQuery-Plugin-For-Easily-Readable-JSON-Data-Viewer.html
     */
    sigma.data = (function() {
        var parse = {};
        /*var nodeColors = {
        'Game': '#617db4',
        'Concept': '#b956af',
        'ItemSet': '#668f3c',
        'Questionnaire': '#c6583e',
        'TAG': '#7F7F7F',
        'Media': '#4488FF'
        }*/

        parse.nodeShape = function(nodeSize) {
            var nodeShapetype = "circle";
            switch (nodeSize) {
                case 1:
                    nodeShapetype = "equilateral";
                    break;
                case 2:
                    nodeShapetype = "circle";
                    break;
                case 3:
                    nodeShapetype = "star";
                    break;
                default:
                    nodeShapetype;
            }
            return nodeShapetype;
        }

        parse.toSigmaJSON = function(inputJsonData) {
            //changing node id's
            var inputArr = inputJsonData.concepts;
            var nodes = [];
            var nodesLength = inputArr.length;
            _.each(inputArr, function(node, index) {
                var identifier = node.identifier.replace(/\:/g, '_');
                node.id = identifier;
                node.x = Math.cos(Math.PI * 2 * index / nodesLength);
                node.y = Math.sin(Math.PI * 2 * index / nodesLength);
                // node.x = Math.random();
                // node.y = Math.random();
                var nodeName = 'Node ' + Math.random();
                if (node.name) {
                    nodeName = node.name;
                } else {
                    nodeName = identifier;
                }

                if (nodeName) {
                    nodeName = (nodeName.length < 70) ? nodeName : nodeName.substr(0, 70);
                }
                node.label = nodeName;
                node.color = color.hexToRgba(jsContants.DEFAULT_COLOR, 1);

                //Defining custom shapes for nodes
                var nodeSize = Math.random();
                if(node.objectType == 'Domain') {
                    node.size = 1; // Default node size
                } else if(node.objectType == 'Dimension') {
                    node.size = 0.12;
                } else {
                    node.size = 0.12;
                }
                var customShape = false;
                if (customShape) {
                    node.type = parse.nodeShape(Math.ceil(3 * nodeSize));
                    //node.size = 1;
                } else {
                    node.type = 'def';
                    //node.size = nodeSize;
                }

                if (node.objectType !== 'AssessmentItem') {
                    //Node object type is not assementItem
                    var alphaVal = customShape == true ? 1 : Math.random();

                    var colorVal = Math.ceil(3 * Math.random());
                    node.color = color.hexToRgba(jsContants.DEFAULT_COLOR, 1);                    

                    nodes.push(node);
                } else {
                    //This is assesmentItem
                    node.size = 0.4;
                    node.color = '#FF00FF';
                }

                if(node.gradeLevel){
                    updateGrades(node);
                    //console.log("gradeLevel: ", node.metadata.gradeLevel);
                }
            });
            _.omit(inputJsonData, 'concepts');
            inputJsonData.nodes = nodes;

            //changing relations/edges id's
            var edges = inputJsonData.relations;
            var nodeEdges = [];
            _.each(edges, function(edge, index) {
                //check startNodeType
                edge.size = 1;
                edge.color = '#F0F0F0';
                edge.originalColor = '#F0F0F0';
                edge.selectedColor = '#AAAAAA';
                
                var startNodeId = edge.startNodeId.replace(/\:/g, '_');
                var endNodeId = edge.endNodeId.replace(/\:/g, '_');
                edge.id = 'e_' + Math.random();
                edge.source = startNodeId;
                edge.target = endNodeId;
                if (edge.relationType === 'associatedTo') {
                    edge.type = 'dashed';
                } else {
                    edge.type = 'arrow';
                }
                nodeEdges.push(edge);
            });
            _.omit(inputJsonData, 'relations');
            inputJsonData.edges = nodeEdges;

            return inputJsonData;
        };

        return parse;
    })();


    /**********************************************************
     *                                                        *
     *                    Node Filter                         *
     *                                                        *
     **********************************************************/
    /**
     * DOM utility functions
     */
    var __ = {
        $: function(id) {
            return document.getElementById(id);
        },

        all: function(selectors) {
            return document.querySelectorAll(selectors);
        },

        removeClass: function(selectors, cssClass) {
            var nodes = document.querySelectorAll(selectors);
            var l = nodes.length;
            for (i = 0; i < l; i++) {
                var el = nodes[i];
                // Bootstrap compatibility
                el.className = el.className.replace(cssClass, '');
            }
        },

        addClass: function(selectors, cssClass) {
            var nodes = document.querySelectorAll(selectors);
            var l = nodes.length;
            for (i = 0; i < l; i++) {
                var el = nodes[i];
                // Bootstrap compatibility
                if (el.className.indexOf(cssClass) === -1) {
                    el.className += ' ' + cssClass;
                }
            }
        },

        show: function(selectors) {
            this.removeClass(selectors, 'hidden');
        },

        hide: function(selectors) {
            this.addClass(selectors, 'hidden');
        },

        toggle: function(selectors, cssClassName) {
            var cssClass = cssClassName || 'hidden';
            var nodes = document.querySelectorAll(selectors);
            var l = nodes.length;
            for (i = 0; i < l; i++) {
                var el = nodes[i];
                //el.style.display = (el.style.display != 'none' ? 'none' : '' );
                // Bootstrap compatibility
                if (el.className.indexOf(cssClass) !== -1) {
                    el.className = el.className.replace(cssClass, '');
                } else {
                    el.className += ' ' + cssClass;
                }
            }
        }
    };

    var filteredObjectTypes = [];

    function applyFilters(defaultMinDegree) {

        function applyMinDegreeFilter(e) {
            var v = e.target.value;
            filterByInputValue(v);
        }

        function filterByInputValue(v) {
            __.$('min-degree-val').textContent = v;

            filter
                .undo('min-degree')
                .nodesBy(function(n) {
                    return this.degree(n.id) >= v;
                }, 'min-degree')
                .apply();
        }

          function filterByGrade(e) {
            if(e.target.selectedIndex === 0){
                filter.undo('grade').apply();
                return;
            }

            var c = e.target[e.target.selectedIndex].text;
            filter
                .undo('grade')
                .nodesBy(function(n) {
                    var returnval = true;
                    if(n.gradeLevel){
                        returnval = n.gradeLevel.indexOf(c);
                        return returnval < 0? false : true;
                    }
                    return returnval;
                }, 'grade')
                .apply();
            }

        function applyCategoryFilter(e) {
            var c = e.target[e.target.selectedIndex].name;
            filter
                .undo('node-category')
                .nodesBy(function(n) {
                    if (n.id === c) {
                        selectNode(n);
                        //Zoom to node
                        //s.cameras[0].goTo({x:n.x, y:n.y, angle: 40, ratio:0.6});

                        //sigma.utils.zoomTo(s.camera, n.x, n.y, 0.4, {duration: 1000}); 
                        var loc = sigma.plugins.locate(s, {
                            animation: {
                                node: {
                                    duration: 1000
                                }
                            },
                            focusOut: true,
                            zoomDef: 1
                        });
                        //loc.center(1); 
                        loc.nodes(n.id, {
                            ratio: 0.4
                        });


                    }
                    return true;
                    //return !c.length || n.metadata.name === c.substr(0, 20);
                }, 'node-category')
                .apply();
        }

        // Filter by node object Type 
        function filterByNodeObjectType(e) {
            var objectTypeFilters = filtersApplied.objectType;
            var isChecked = $(e.target).prop('checked');
            var selectedObjectType = $(e.target).val();

            var alreadyAdded = false;
            if (isChecked) {
                alreadyAdded = _.findWhere(objectTypeFilters, selectedObjectType);
                if (!alreadyAdded) {
                    objectTypeFilters.push(selectedObjectType);
                }
            } else {
                var index = _.indexOf(objectTypeFilters, selectedObjectType);
                objectTypeFilters.splice(index, 1);
            }
            filtersApplied.objectType = objectTypeFilters;

            filter
                .undo('node-object-type')
                .nodesBy(function(n) {
                    var showThisNode = false;
                    _.each(objectTypeFilters, function(filteredObjectType) {
                        if (n.objectType === filteredObjectType) {
                            showThisNode = true;
                            return showThisNode;
                        }
                    });
                    return showThisNode;
                }, 'node-object-type')
                .apply();
        }

        // Filter by node object Type 
        function filterByNodeRelationType(e) {
            var relationTypeFilters = filtersApplied.relationType;
            var isChecked = $(e.target).prop('checked');
            var selectedObjectType = $(e.target).val();

            var alreadyAdded = false;
            if (isChecked) {
                alreadyAdded = _.findWhere(relationTypeFilters, selectedObjectType);
                if (!alreadyAdded) {
                    relationTypeFilters.push(selectedObjectType);
                }
            } else {
                var index = _.indexOf(relationTypeFilters, selectedObjectType);
                relationTypeFilters.splice(index, 1);
            }
            filtersApplied.relationType = relationTypeFilters;

            filter
                .undo('node-relation-type')
                .edgesBy(function(edge) {
                    var showThisNode = false;
                    _.each(relationTypeFilters, function(filteredObjectType) {
                        if (edge.relationType === filteredObjectType) {
                            showThisNode = true;
                            return showThisNode;
                        }
                    });
                    return showThisNode;
                }, 'node-relation-type')
                .apply();
        }

        __.$('node-category').addEventListener('change', applyCategoryFilter);
        __.$('gradesDropdown').addEventListener('change', filterByGrade);
        $('#listObjectTypes input').bind('change', filterByNodeObjectType);
        $('#listRelationTypes input').bind('change', filterByNodeRelationType);



        // reset button
        __.$('reset-btn').addEventListener('click', function() {
            //__.$('min-degree').value = 0;
            //__.$('min-degree-val').textContent = '0';
            $('#listObjectTypes li input').each(function() {
                $(this).prop('checked', false);
            });

            $('#listRelationTypes li input').each(function() {
                $(this).prop('checked', false);
            });

            filtersApplied.objectType = [];
            filtersApplied.relationType = [];
            deleteRecos();

            s.settings({
                maxEdgeSize: 2
            });
            resetGraph();
            $('#learnerId').val('');
            $('#learnerSnapshotDetails').empty();
            $('#relevanceSubmit').removeClass('btn-selected').hide();
            __.$('node-category').selectedIndex = 0;
            __.$('gradesDropdown').selectedIndex = 0;
            filterInput.undo().apply();
            __.$('dump').textContent = '';
            __.hide('#dump');
        });
        (function() {
            if (defaultMinDegree) {
                filterByInputValue(defaultMinDegree);
                __.$('min-degree').value = defaultMinDegree;
            }
        })();
    }

    function doFilterByObjectType(input) {
        if (_.isArray(input)) {

        } else if (_.isString(input)) {

        }
    }

    //Adding node Object_Types to the list(Checkbox list)
    //Adding node Relation_Types to the list
    function loadFilterTypes() {
        $('#listObjectTypes').empty();
        $('#listRelationTypes').empty();
        var nodes = s.graph.nodes();
        var uniqueObjectTypes = _.uniq(nodes, false, function(node) {
            if (node.objectType != null) {
                //console.log(node.id + "  " + node.objectType);
                return node.objectType;
            }
        });

        _.each(uniqueObjectTypes, function(node) {
            if (node.objectType != null) {
                $('#listObjectTypes').append('<li class="checkbox checkbox-primary"><input id=cbx_"' + node.nodeType + '" type="checkbox" class="checkbox" data-toggle="checkbox-x" value="' + node.objectType + '"/><label for="cbx_' + node.nodeType + '">' + node.objectType + '</label></li>');
            }
        });

        var edges = s.graph.edges();
        var uniqueRelationTypes = _.uniq(edges, false, function(edge) {
            if (edge.relationType != null) {
                //console.log(edge.id + "  " + edge.relationType);
                return edge.relationType;
            }
        });

        _.each(uniqueRelationTypes, function(edge) {
            $('#listRelationTypes').append('<li class="checkbox checkbox-primary"><input id=cbx_"' + edge.relationType + '" type="checkbox" class="checkbox" data-toggle="checkbox-x" value="' + edge.relationType + '"/><label for="cbx_' + edge.relationType + '">' + edge.relationType + '</label></li>');
        });
    }

    function addGradesToDropdown(){
        $($('#gradesDropdown option')[0]).siblings().remove();
        var gradesDropdownEle = __.$('gradesDropdown');
        _grades.sort();
        _grades.forEach(function(c) {
            var optionElt = document.createElement('option');
            optionElt.text = c;
            gradesDropdownEle.add(optionElt);
            //$("#gradesDropdown").append('<option>'+ c +'</option>');
        });
    }


    function updatePane(graph, filterInput) {
        // get max degree
        var maxDegree = 0,
            categories = [],
            _grades = [];
        // read nodes
        var nodes = graph.nodes();
        nodes.forEach(function(n) {
            maxDegree = Math.max(maxDegree, graph.degree(n.id));

            var simpleNodeObj = {
                'id': n.id,
                'name': n.label
            };
            categories.push(simpleNodeObj);
            //categories[n.label] = true;
        });
        // min degree
        //__.$('min-degree').max = maxDegree;
        //__.$('max-degree-value').textContent = maxDegree;
        // node names

        var nodecategoryElt = __.$('node-category');
        var nodeNames = _.sortBy(categories, 'name') //Object.keys(categories).sort();
        nodeNames.forEach(function(c) {
            var optionElt = document.createElement('option');
            optionElt.text = c.name.substr(0, 70);
            optionElt.name = c.id;
            //optionElt.style.width = "100%";
            nodecategoryElt.add(optionElt);
        });

        $('#node-category option').width(250);

        //To Add the the grades to dropdown element
        addGradesToDropdown();

        graphLoadTimeout = setTimeout(function() {
            /*s.graph.nodes().forEach(function(e) {
                e.size = sigma.settings.maxNodeSize / 2;
            });*/
            // Gave delay to stop force layout
            clearTimeout(graphLoadTimeout);
            s.refresh();
            s.stopForceAtlas2();
        }, jsContants.FORCE_LAYOUT_STOP_TIMER);
    }


    // Instantiate sigma:
    var jsonData = sigma.data.toSigmaJSON(graphData);
    s = new sigma({
        graph: jsonData,
        renderer: {
            // IMPORTANT:
            // This works only with the canvas renderer, so the
            // renderer type set as "canvas" is necessary here.
            container: document.getElementById('sigmaGraph'),
            type: 'canvas'
        },
        settings: {
            minNodeSize: 4,
            maxNodeSize: 20,
            minEdgeSize: 0.5,
            maxEdgeSize: 2,
            enableEdgeHovering: true,
            edgeHoverSizeRatio: 2,
            labelSizeRatio: 0.3,
            labelThreshold: 20,
            minArrowSize: 4,
            edgeLabelSize: 'proportional',
            autoRescale: true,
            zoomingRatio: 1.7,
            zoomMin: 0.001,
            zoomMax: 2
        }
    });

    s.startForceAtlas2({
        adjustSizes: true,
        strongGravityMode: true,
        gravity: 0.6,
        slowDown: 0.4
    });
    //s.stopForceAtlas2();
    //new sigma.plugins.neighborhoods();
    //sigma.renderers.def = sigma.renderers.canvas;

    //To drag nodes
    allowDrag(s);

    //Bind click event to highlight edges on node click
    hightlightConnections(s);

    // Initialize the Filter API
    filter = new sigma.plugins.filter(s);
    updatePane(s.graph, filter);
    loadFilterTypes();
    applyFilters();
    updateProficiency();
}

function resetGraph(){
    filter.undo().apply();
    sigma.plugins.locate(s).center(1);
    s.stopForceAtlas2();
    s.graph.edges().forEach(function(e) {
        e.color = e.originalColor;
    });
    s.graph.nodes().forEach(function(e) {
        resetNode(e);
    });
    s.refresh();
}

/* 
 * ********* Generic function ************
 */

var color = {
    hexToRgb: function(hexCode) {
        //Converting hex code color to rgba color
        var result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hexCode);
        var rgbColor = {
            r: parseInt(result[1], 16),
            g: parseInt(result[2], 16),
            b: parseInt(result[3], 16)
        };
        var rgbColor = 'rgba(' + rgbColor.r + ', ' + rgbColor.g + ', ' + rgbColor.b + ')';
        return rgbColor;
    },
    hexToRgba: function(hexCode, alpha) {
        //Converting hex code color to rgba color
        try {
            var result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hexCode);
            var rgbColor = {
                r: parseInt(result[1], 16),
                g: parseInt(result[2], 16),
                b: parseInt(result[3], 16)
            };
            var rgbaColor = 'rgba(' + rgbColor.r + ', ' + rgbColor.g + ', ' + rgbColor.b + ', ' + alpha + ')';
        } catch (e) {
            console.log(rgbaColor);
        }
        return rgbaColor;
    }
}

$(document).ready(function() {
    $('#relevanceSubmit').hide();
    loadGraph('numeracy');
    //loadLearnerProfData();

    $('#proficiencySubmit').bind('click', function(e) {
        $('#relevanceSubmit').removeClass('btn-selected');

        if($(this).hasClass('btn-selected')){
            //$('#relevanceSubmit').hide();
        }else{
            getLearnerInfo();
        }
        //$(this).toggleClass('btn-selected');
    });

    $('#relevanceSubmit').bind('click', function(e) {
        if($(this).hasClass('btn-selected')){
            //$(this).val('Show Recommends');
            deleteRecos();
            s.refresh();
        }else{
            //$(this).val('Hide Recommends');
            showRecommendations(_learnerInfo.recos);
        }
        $(this).toggleClass('btn-selected');
        $(this).focusout();
    });
});

var _grades = [];
function updateGrades(node){
    if(node.gradeLevel){
        _grades = _.union(_grades, node.gradeLevel);            
    }
}

function showRecommendations(recos) {
    //resetGraph();
    deleteRecos();
    if(recos) {
        s.settings({
            maxEdgeSize: 5
        });
        var nodes = s.graph.nodes();
        var startNodeId = recos.startNode.cid;
        var endNodes = recos.endNodes;

        endNodes.forEach(function(node, idx) {
            var nodeFound = _.findWhere(nodes, {
                'identifier': node.cid
            });
            if (nodeFound && (node.cid !== startNodeId)) {
                if(!nodeFound.proficiency){
                    nodeFound.size = 0.4;
                    nodeFound.color = '#617db4';                    
                }
            }
            var id = 'e_' + Math.random();
            recoEdges.push(id);
            s.graph.addEdge({
                id: id,
                source: startNodeId.replace(/\:/g, '_'),
                target: node.cid.replace(/\:/g, '_'),
                size: idx,
                type: 'curvedArrow',
                color: '#b956af',
                originalColor: '#b956af'
            });
        })
        var nodeFound = _.findWhere(nodes, {
            'identifier': startNodeId
        });
        s.refresh();
    }
}

function deleteRecos() {
    s.settings({
        maxEdgeSize: 2
    });
    if(_learnerInfo.recos){
        var nodes = s.graph.nodes();
        var startNodeId = _learnerInfo.recos.startNode.cid;
        var endNodes = _learnerInfo.recos.endNodes;

        endNodes.forEach(function(node, idx) {
        var nodeFound = _.findWhere(nodes, {
                    'identifier': node.cid
                });
                if (nodeFound && (node.cid !== startNodeId)) {
                    if(!nodeFound.proficiency){   
                        resetNode(nodeFound);
                    }
                }
        });
        _.each(recoEdges, function(id) {
            s.graph.dropEdge(id);
        });
        s.refresh();      
        recoEdges = [];  
    }
}
