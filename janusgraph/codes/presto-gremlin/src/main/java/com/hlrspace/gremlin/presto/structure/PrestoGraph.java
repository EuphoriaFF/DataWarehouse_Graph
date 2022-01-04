package com.hlrspace.gremlin.presto.structure;

import com.hlrspace.gremlin.presto.connector.*;
import com.hlrspace.gremlin.presto.util.JDBCUtils;
import com.hlrspace.gremlin.presto.util.QueryUtils;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
public class PrestoGraph implements Graph {

    protected SessionManager sessionManager;
    protected BaseConfiguration configuration = new BaseConfiguration();

    protected Features features = new PrestoGraphFeatures();

    protected List<String> edgeLabels;
    protected List<String> vertexLabels;
    protected Map<String, List<String>> vertexInfo;
    protected Map<String, List<String>> edgeInfo;

    public static final String CONFIG_CONTACT_POINT = "contact-point";
    public static final String CONFIG_PORT = "port";
    public static final String CONFIG_CATALOG = "catalog";
    public static final String CONFIG_USER = "user";

    private PrestoGraph(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
        try {
            init();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void init() throws SQLException {
        edgeInfo = new HashMap<>();
        vertexInfo = new HashMap<>();
        //初始化vertexLabel 和 edgeLabel
        List<HashMap<String, Object>> rsEdgeLabel = null;
        List<HashMap<String, Object>> rsVertexLabel = null;
        List<HashMap<String, Object>> rsVertexInfo = null;
        List<HashMap<String, Object>> rsEdgeInfo = null;

        try {
            rsEdgeLabel = JDBCUtils.convertRresultSetToListMap(sessionManager.session().execute(QueryUtils.edgeLabel()));
            rsVertexLabel = JDBCUtils.convertRresultSetToListMap(sessionManager.session().execute(QueryUtils.vertexLabel()));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        edgeLabels = rsEdgeLabel.stream().flatMap(e -> e.values().stream()).map(o -> (String) o).collect(Collectors.toList());
        vertexLabels = rsVertexLabel.stream().flatMap(e -> e.values().stream()).map(o -> (String) o).collect(Collectors.toList());

        //初始化edgeInfo
        for (String label : edgeLabels) {
            edgeInfo.put(label,
                    JDBCUtils.convertRresultSetToListMap(sessionManager.session().execute(QueryUtils.idByLabel(label)))
                            .stream()
                            .flatMap(e -> e.values().stream())
                            .map(o -> (String) o)
                            .collect(Collectors.toList())
            );
        }

        //初始化vertexInfo
        for (String label : vertexLabels) {
            vertexInfo.put(label,
                    JDBCUtils.convertRresultSetToListMap(sessionManager.session().execute(QueryUtils.idByLabel(label)))
                            .stream()
                            .flatMap(e -> e.values().stream())
                            .map(o -> (String) o)
                            .collect(Collectors.toList())
            );
        }

    }



    public static PrestoGraph open(final Configuration configuration) {
        //建立和数据库的联系
        String contactPoint = configuration.getString(CONFIG_CONTACT_POINT);
        int port = configuration.getInt(CONFIG_PORT);
        String catalog = configuration.getString(CONFIG_CATALOG);
        String user = configuration.getString(CONFIG_USER);
        Cluster prestoCluster = PrestoCluster.builder()
                .addContactPoint(contactPoint)
                .withPort(port)
                .catalog(catalog)
                .setProperty("user", user)
                .build();
        Session session = prestoCluster.connect();
        SessionManager sessionManager = new PrestoSessionManager(session);
        return new PrestoGraph(sessionManager);
    }

    @Override
    public void close() throws Exception {
        sessionManager.session().close();
    }


    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        Set<Object> set = new HashSet<>(Arrays.asList(vertexIds));
        List<Vertex> vertices = new ArrayList<>();
        vertexInfo.entrySet()
                .stream()
                .forEach(e -> {
                    String label = e.getKey();
                    e.getValue()
                            .stream()
                            .filter(s -> set.size() == 0 || set.contains(s))
                            .forEach(id -> vertices.add(new PrestoVertex(id, label, this, this.sessionManager)));
                });
        return vertices.iterator();
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        Set<Object> set = new HashSet<>(Arrays.asList(edgeIds));
        List<Edge> edges = new ArrayList<>();
        edgeInfo.entrySet()
                .stream()
                .forEach(e -> {
                    String label = e.getKey();
                    e.getValue()
                            .stream()
                            .filter(s -> set.size() == 0 || set.contains(s))
                            .forEach(id -> edges.add(new PrestoEdge(id, label, this, this.sessionManager)));
                });
        return edges.iterator();
    }


    @Override
    public Vertex addVertex(Object... keyValues) {
        throw Graph.Exceptions.vertexAdditionsNotSupported();
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Transaction tx() {
        throw Graph.Exceptions.transactionsNotSupported();
    }

    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    @Override
    public Configuration configuration() {
        return this.configuration;
    }

    @Override
    public Features features() {
        return features;
    }

    public class PrestoGraphFeatures implements Features {

        protected GraphFeatures graphFeatures = new PrestoGraphGraphFeatures();
        protected VertexFeatures vertexFeatures = new PrestoGraphVertexFeatures();
        protected EdgeFeatures edgeFeatures = new PrestoGraphEdgeFeatures();

        @Override
        public GraphFeatures graph() {
            return graphFeatures;
        }

        @Override
        public VertexFeatures vertex() {
            return vertexFeatures;
        }

        @Override
        public EdgeFeatures edge() {
            return edgeFeatures;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }

        public class PrestoGraphGraphFeatures implements GraphFeatures {

            @Override
            public VariableFeatures variables() {
                return new VariableFeatures() {
                    @Override
                    public boolean supportsVariables() {
                        return false;
                    }
                };
            }

            @Override
            public boolean supportsComputer() {
                return false;
            }

            @Override
            public boolean supportsPersistence() {
                return false;
            }

            @Override
            public boolean supportsConcurrentAccess() {
                return false;
            }

            @Override
            public boolean supportsTransactions() {
                return false;
            }

            @Override
            public boolean supportsThreadedTransactions() {
                return false;
            }

            @Override
            public boolean supportsIoRead() {
                return false;
            }

            @Override
            public boolean supportsIoWrite() {
                return false;
            }
        }

        public class PrestoElementFeatures implements ElementFeatures {


            @Override
            public boolean supportsNullPropertyValues() {
                return true;
            }

            @Override
            public boolean supportsAddProperty() {
                return false;
            }

            @Override
            public boolean supportsRemoveProperty() {
                return false;
            }

            @Override
            public boolean supportsUserSuppliedIds() {
                return true;
            }

            @Override
            public boolean supportsNumericIds() {
                return false;
            }

            @Override
            public boolean supportsStringIds() {
                return true;
            }

            @Override
            public boolean supportsUuidIds() {
                return false;
            }

            @Override
            public boolean supportsCustomIds() {
                return false;
            }

            @Override
            public boolean supportsAnyIds() {
                return false;
            }

            @Override
            public boolean willAllowId(Object id) {
                return true;
            }
        }

        public class PrestoGraphVertexFeatures extends PrestoElementFeatures implements VertexFeatures {

            @Override
            public VertexProperty.Cardinality getCardinality(String key) {
                return VertexProperty.Cardinality.single;
            }

            @Override
            public boolean supportsAddVertices() {
                return false;
            }

            @Override
            public boolean supportsRemoveVertices() {
                return false;
            }

            @Override
            public boolean supportsMultiProperties() {
                return true;
            }

            @Override
            public boolean supportsDuplicateMultiProperties() {
                return false;
            }

            @Override
            public boolean supportsMetaProperties() {
                return false;
            }

            @Override
            public boolean supportsUpsert() {
                return false;
            }
        }

        public class PrestoGraphEdgeFeatures extends PrestoElementFeatures implements EdgeFeatures {

            EdgePropertyFeatures edgePropertyFeatures = new PrestoEdgePropertyFeatures();

            @Override
            public EdgePropertyFeatures properties() {
                return edgePropertyFeatures;
            }

            @Override
            public boolean supportsAddEdges() {
                return false;
            }

            @Override
            public boolean supportsRemoveEdges() {
                return false;
            }

            @Override
            public boolean supportsUpsert() {
                return false;
            }

        }

        public class PrestoEdgePropertyFeatures implements EdgePropertyFeatures {
            @Override
            public boolean supportsBooleanValues() {
                return false;
            }

            @Override
            public boolean supportsByteValues() {
                return false;
            }

            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsBooleanArrayValues() {
                return false;
            }

            @Override
            public boolean supportsByteArrayValues() {
                return false;
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return false;
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return false;
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return false;
            }

            @Override
            public boolean supportsStringArrayValues() {
                return false;
            }

            @Override
            public boolean supportsLongArrayValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }
        }

        public class PrestoVertexPropertyFeatures extends PrestoElementFeatures implements VertexPropertyFeatures {
            @Override
            public boolean supportsBooleanValues() {
                return false;
            }

            @Override
            public boolean supportsByteValues() {
                return false;
            }

            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsBooleanArrayValues() {
                return false;
            }

            @Override
            public boolean supportsByteArrayValues() {
                return false;
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return false;
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return false;
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return false;
            }

            @Override
            public boolean supportsStringArrayValues() {
                return false;
            }

            @Override
            public boolean supportsLongArrayValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }
        }

    }
}
