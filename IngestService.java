package net.samsung.ds.injest.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import net.samsung.ds.injest.config.IgniteMode;
import net.samsung.ds.injest.model.TableDTO;
import org.apache.commons.lang.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes.TcpDiscoveryKubernetesIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.springframework.util.ObjectUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class IgniteService {

    private final Properties properties;

    public IgniteService(Properties properties) {
        this.properties = properties;
    }

    private IgniteConfiguration initConfiguration() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        IgniteMode igniteMode = IgniteMode.valueOf(properties.get("ignite.mode").toString());

        switch (igniteMode) {
            case Multicast:
                cfg.setClientMode(true);
                cfg.setPeerClassLoadingEnabled(true);
                TcpDiscoveryMulticastIpFinder tcpDiscoveryMulticastIpFinder = new TcpDiscoveryMulticastIpFinder();
                tcpDiscoveryMulticastIpFinder.setAddresses(Collections.singletonList(properties.getProperty("ignite.mode.multicast.address")));
                cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(tcpDiscoveryMulticastIpFinder));
                return cfg;
            default:
                TcpDiscoveryKubernetesIpFinder tcpDiscoveryKubernetesIpFinder = new TcpDiscoveryKubernetesIpFinder();
                tcpDiscoveryKubernetesIpFinder.setNamespace(properties.getProperty("ignite.k8s.name-space","yms-example"));
                tcpDiscoveryKubernetesIpFinder.setServiceName(properties.getProperty("ignite.k8s.service-name","apache-ignite"));
                cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(tcpDiscoveryKubernetesIpFinder));
                cfg.setClientMode(true);
                return cfg;
        }

    }

    public IgniteCache<?, ?> igniteCache() {
        Ignite ignite = Ignition.start(initConfiguration());
        ignite.cluster().active(true);

        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(properties.getProperty("ignite.cache-name","dummy_cache"))
                .setSqlSchema(properties.getProperty("ignite.cache-name.schema","PUBLIC"));

        IgniteCache<?, ?> cache = ignite.getOrCreateCache(cacheCfg);
        return cache;
    }

    public void createTable(String key, String message, IgniteCache cache) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            List<TableDTO> columnData = Arrays.asList(objectMapper.readValue(message, TableDTO[].class));
            StringBuffer query = new StringBuffer("CREATE TABLE IF NOT EXISTS ").append(key).append("(");
            ArrayList<String> columns = new ArrayList<>();
            ArrayList<String> primaries = new ArrayList<>();
            columnData.forEach(table -> {
                if (!ObjectUtils.isEmpty(table.getIsPrimaryKey())) primaries.add(table.getColumn());
                columns.add(table.getColumn() + " " + table.getDataType());
            });
            query.append(StringUtils.join(columns,","));
            if (primaries.isEmpty()) {
                query.append(")");
            } else {
                query.append("PRIMARY KEY(").append(StringUtils.join(primaries,",")).append("))");
            }
            SqlFieldsQuery qry = new SqlFieldsQuery(query.toString());
            cache.query(qry).getAll();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }

    public void tableInfo(String key, String message, IgniteCache cache){
        JsonParser jsonParser = new JsonParser();
        JsonArray jsonArray = (JsonArray) jsonParser.parse(message);

        String colVal = "";

        ArrayList<String> priList = new ArrayList<>();

        for (int i = 0; i < jsonArray.size(); i++) {
            JsonObject object = (JsonObject) jsonArray.get(i);
            String column = object.get("column").getAsString();
            String dataType = object.get("dataType").getAsString();

            if (object.get("isPrimaryKey") != null) {
                priList.add(column);
            }
            colVal += column + " " + dataType + ", ";
        }
        SqlFieldsQuery qry = new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS " + key +
                "(" + colVal + "PRIMARY KEY(" + StringUtils.join(priList,",") + "))" );
//        System.out.println("query>>>"+qry.getSql());
        cache.query(qry).getAll();
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>> table "+ key + " created! >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
    }


    public void tableIndex(String key, String message, IgniteCache cache){
        JsonParser jsonParser = new JsonParser();
        JsonArray jsonArray = (JsonArray) jsonParser.parse(message);

        for (int i = 0; i < jsonArray.size(); i++) {
            JsonObject object = (JsonObject) jsonArray.get(i);
            String name = object.get("name").getAsString();
            String column = object.get("column").getAsString();
            String inline_size = object.get("inline_size").getAsString();

            SqlFieldsQuery qry = new SqlFieldsQuery("CREATE INDEX " + name + " ON " + key + " ( " + column + " ) INLINE_SIZE " + inline_size );
            System.out.println("query>>>"+qry.getSql());
            cache.query(qry).getAll();
            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>> table " + key + " index " + name + " created! >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        }

    }

    public void resultData(String key, String message, IgniteCache cache){
        long tot_start_time = System.currentTimeMillis();

        Connection conn = null;
        JsonParser jsonParser = new JsonParser();
        JsonArray jsonArray = (JsonArray) jsonParser.parse(message);
        int rowSize = jsonArray.size();
        int columnSize = 0;

        if (rowSize>0){
            JsonArray columnArray = (JsonArray) jsonParser.parse(jsonArray.get(0).toString());
            columnSize = columnArray.size();
        }
        List qList = new ArrayList();
        for (int i=0; i < columnSize; i++){
            qList.add("?");
        }
        String qryVal = StringUtils.join(qList,",");
        String query = "INSERT INTO "+key+" VALUES( " +qryVal+ ")";
//        System.out.println("query>>"+query);
        try {
            conn = DriverManager.getConnection(properties.getProperty("ignite.data-source"));
            PreparedStatement stmt = conn.prepareStatement("SET STREAMING ON;");
            boolean res = stmt.execute();
            PreparedStatement stmt2 = conn.prepareStatement(query);
            for (int i = 0; i < rowSize; i++) {
                JsonArray jsonArray2 = (JsonArray) jsonParser.parse(jsonArray.get(i).toString());
                for (int k = 0; k < jsonArray2.size(); k++) {
                    //System.out.println(jsonArray2.get(k).getAsString());
                    stmt2.setObject(k + 1, jsonArray2.get(k).getAsString());
                }
                boolean res2 = stmt2.execute();
            }
            System.out.println(key + " Table" + rowSize + "Rows Inserted!");
            stmt2.close();
            conn.close();
        } catch(SQLException ex){
            System.out.println("SQLException:"+ex);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long tot_end_time = System.currentTimeMillis();
        long during_time = (tot_end_time - tot_start_time) / 1000;
//                    System.out.println("now>>>>"+getCurrentDateTime());
//                    System.out.println("tot_start_time>>>>"+simpleDateFormat.format(tot_start_time));
//                    System.out.println("tot_end_time>>>>"+simpleDateFormat.format(tot_end_time));
//                    System.out.println("during_time>>>>"+during_time);
//                    System.out.println(">>> Loaded " + 10000 + " keys in " + (tot_end_time - tot_start_time) / 1000 + "s.");
        SqlFieldsQuery qry = null;
        qry = new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS TB_LOG (START_TIME TIMESTAMP PRIMARY KEY , END_TIME TIMESTAMP,TAB VARCHAR, SIZE INT, DURING_TIME INT)");
        cache.query(qry).getAll();
        qry = null;
        qry = new SqlFieldsQuery("INSERT INTO TB_LOG VALUES ('" + simpleDateFormat.format(tot_start_time) + "','" + simpleDateFormat.format(tot_end_time) + "','" + key + "'," + rowSize + "," + during_time + ")");
        cache.query(qry).getAll();
    }


}
