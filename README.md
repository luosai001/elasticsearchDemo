# elasticsearchDemo
elasticsearch 全文检索demo,geo搜索附近的人
##
    // 创建索引
    public static void createIndex(String indexName, String indexType) throws IOException {
        Client esClient = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        // 创建Mapping
        XContentBuilder mapping = createMapping(indexType);
        System.out.println("mapping:" + mapping.string());
        // 创建一个空索引
        esClient.admin().indices().prepareCreate(indexName).execute().actionGet();
        PutMappingRequest putMapping = Requests.putMappingRequest(indexName).type(indexType).source(mapping);
        PutMappingResponse response = esClient.admin().indices().putMapping(putMapping).actionGet();
        if (!response.isAcknowledged()) {
            System.out.println("Could not define mapping for type [" + indexName + "]/[" + indexType + "].");
        } else {
            System.out.println("Mapping definition for [" + indexName + "]/[" + indexType + "] succesfully created.");
        }
    }

    // 创建mapping
    public static XContentBuilder createMapping(String indexType) {
        XContentBuilder mapping = null;
        try {
            mapping = jsonBuilder().startObject()
                    // 索引库名（类似数据库中的表）
                    .startObject(indexType).startObject("properties")
                    // ID
                    .startObject("id").field("type", "long").endObject()
                    // 姓名
                    .startObject("name").field("type", "string").endObject()
                    // 位置
                    .startObject("location").field("type", "geo_point").endObject()
                    .endObject().endObject().endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mapping;
    }

    // 添加数据
    public static Integer addIndexData100000(String indexName, String indexType) throws UnknownHostException {
        Client client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        List<String> cityList = new ArrayList<String>();

        double lat = 39.929986;
        double lon = 116.395645;
        for (int i = 100000; i < 1000000; i++) {
            double max = 0.00001;
            double min = 0.000001;
            Random random = new Random();
            double s = random.nextDouble() % (max - min + 1) + max;
            DecimalFormat df = new DecimalFormat("######0.000000");
            // System.out.println(s);
            String lons = df.format(s + lon);
            String lats = df.format(s + lat);
            Double dlon = Double.valueOf(lons);
            Double dlat = Double.valueOf(lats);

            User city1 = new User(i, "郭德纲"+i, dlat, dlon);
            cityList.add(obj2JsonUserData(city1));
        }
        // 创建索引库
        List<IndexRequest> requests = new ArrayList<IndexRequest>();
        for (int i = 0; i < cityList.size(); i++) {
            IndexRequest request = client.prepareIndex(indexName, indexType).setSource(cityList.get(i)).request();
            requests.add(request);
        }

        // 批量创建索引
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (IndexRequest request : requests) {
            bulkRequest.add(request);
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            System.out.println("批量创建索引错误！");
        }
        return bulkRequest.numberOfActions();
    }


    public static String obj2JsonUserData(User user) {
        String jsonData = null;
        try {
            // 使用XContentBuilder创建json数据
            XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
            jsonBuild.startObject().field("id", user.getId()).field("name", user.getName()).startArray("location").value(user.getLon()).value(user.getLat()).endArray()
                    .endObject();
            jsonData = jsonBuild.string();
            System.out.println(jsonData);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jsonData;
    }

    // 获取附近的人
    public static void testGetNearbyPeople(Client client, String index, String type, double lat, double lon) {
        SearchRequestBuilder srb = client.prepareSearch(index).setTypes(type);
        srb.setFrom(0).setSize(1000);//1000人
        // lon, lat位于谦的坐标，查询距离于谦1米到1000米
//        FilterBuilder builder = geoDistanceRangeFilter("location").point(lon, lat).from("1m").to("100m").optimizeBbox("memory").geoDistance(GeoDistance.PLANE);
        GeoDistanceQueryBuilder location1 = QueryBuilders.geoDistanceQuery("location").point(lat,lon).distance(80,DistanceUnit.METERS);
        srb.setPostFilter(location1);
        // 获取距离多少公里 这个才是获取点与点之间的距离的
        GeoDistanceSortBuilder sort = SortBuilders.geoDistanceSort("location",lat,lon);
        sort.unit(DistanceUnit.METERS);
        sort.order(SortOrder.ASC);
        sort.point(lat,lon);
        srb.addSort(sort);

        SearchResponse searchResponse = srb.execute().actionGet();

        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHists = hits.getHits();
        // 搜索耗时
        Float usetime = searchResponse.getTookInMillis() / 1000f;
        System.out.println("于谦附近的人(" + hits.getTotalHits() + "个)，耗时("+usetime+"秒)：");
        for (SearchHit hit : searchHists) {
            String name = (String) hit.getSource().get("name");
            List<Double> location = (List<Double>)hit.getSource().get("location");
            // 获取距离值，并保留两位小数点
            BigDecimal geoDis = new BigDecimal((Double) hit.getSortValues()[0]);
            Map<String, Object> hitMap = hit.getSource();
            // 在创建MAPPING的时候，属性名的不可为geoDistance。
            hitMap.put("geoDistance", geoDis.setScale(0, BigDecimal.ROUND_HALF_DOWN));
            System.out.println(name+"的坐标："+location + "他距离于谦" + hit.getSource().get("geoDistance") + DistanceUnit.METERS.toString());
        }

    }
    public static void main(String[] args) throws IOException {
        Client client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        String index = "es";
        String type = "people";
//        createIndex(index, type);
//        addIndexData100000(index, type);

        double lat = 39.929986;
        double lon = 116.395645;
        long start = System.currentTimeMillis();
        //query("郭", index, type);
        testGetNearbyPeople(client, index, type, lat, lon);
        long end = System.currentTimeMillis();
        System.out.println((end - start) + "毫秒");
        //client.close();// 1.5.2用完不用关闭
    }
