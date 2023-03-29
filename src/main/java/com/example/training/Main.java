package com.example.training;

import com.github.luben.zstd.ZstdInputStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@RequiredArgsConstructor
public class Main {

    private static final Pattern DATE_PATTERN = Pattern.compile("\\d{2}\\.\\d{2}\\.\\d{4}");
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("dd.MM.yyyy");
    public static final ZoneId ZONE_ID = ZoneId.of("Europe/Moscow");
    public static final String COLLECTION = "archive_case";

    public static void main(String[] args) throws IOException, SolrServerException, ParseException {
        try (
                InputStream is = Files.newInputStream(Paths.get("/home/user/Загрузки/суды/berkeley.jsonl.zst"));
                BufferedReader reader = new BufferedReader(new InputStreamReader(new ZstdInputStream(is)));
                SolrClient client = new CloudLegacySolrClient.Builder(List.of("10.2.0.10:2181"), Optional.empty()).build()
        ) {

            String line;
            List<SolrInputDocument> batch = new ArrayList<>();
            while ((line = reader.readLine()) != null) {
                if (line.contains("div")) {
                    Document document = Jsoup.parse(line);
                    String head = document.select(".judge_header").html();
                    String card = document.select(".judge_card").html();
                    String region = head.substring(head.lastIndexOf("(") + 1, head.lastIndexOf(")"));
                    SolrInputDocument inputDocument = new SolrInputDocument();
                    inputDocument.addField("id", UUID.nameUUIDFromBytes(line.getBytes()).toString());
                    inputDocument.addField("region", getRegion(region));
                    inputDocument.addField("head", head);
                    inputDocument.addField("card", card);
                    batch.add(inputDocument);
                    if (batch.size() == 1000) {
                        client.add(COLLECTION, batch, 60000);
                        log.info("success commit {} docs", batch.size());
                        batch = new ArrayList<>();
                    }
                }
            }
            client.add(COLLECTION, batch);
            client.commit(COLLECTION);
            log.info("success finally commit with {} docs", batch.size());
        }
    }



    protected static String getRegion(String name) {
        HashMap<String, String> map = new HashMap<>();
        map.put("Республика Адыгея (Адыгея)", "01");
        map.put("Республика Башкортостан", "02");
        map.put("Республика Бурятия", "03");
        map.put("Республика Алтай", "04");
        map.put("Республика Дагестан", "05");
        map.put("Республика Ингушетия", "06");
        map.put("Кабардино - Балкарская Республика", "07");
        map.put("Республика Калмыкия", "08");
        map.put("Карачаево - Черкесская Республика", "09");
        map.put("Республика Карелия", "10");
        map.put("Республика Коми", "11");
        map.put("Республика Марий Эл", "12");
        map.put("Республика Мордовия", "13");
        map.put("Республика Саха (Якутия)", "14");
        map.put("Республика Северная Осетия - Алания", "15");
        map.put("Республика Татарстан (Татарстан)", "16");
        map.put("Республика Тыва", "17");
        map.put("Удмуртская Республика", "18");
        map.put("Республика Хакасия", "19");
        map.put("Чеченская Республика", "20");
        map.put("Чувашская Республика -Чувашия", "21");
        map.put("Алтайский край", "22");
        map.put("Краснодарский край", "23");
        map.put("Красноярский край", "24");
        map.put("Приморский край", "25");
        map.put("Ставропольский край", "26");
        map.put("Хабаровский край", "27");
        map.put("Амурская область", "28");
        map.put("Архангельская область", "29");
        map.put("Астраханская область", "30");
        map.put("Белгородская область", "31");
        map.put("Брянская область", "32");
        map.put("Владимирская область", "33");
        map.put("Волгоградская область", "34");
        map.put("Вологодская область", "35");
        map.put("Воронежская область", "36");
        map.put("Ивановская область", "37");
        map.put("Иркутская область", "38");
        map.put("Калининградская область", "39");
        map.put("Калужская область", "40");
        map.put("Камчатский край", "41");
        map.put("Кемеровская область", "42");
        map.put("Кировская область", "43");
        map.put("Костромская область", "44");
        map.put("Курганская область", "45");
        map.put("Курская область", "46");
        map.put("Ленинградская область", "47");
        map.put("Липецкая область", "48");
        map.put("Магаданская область", "49");
        map.put("Московская область", "50");
        map.put("Мурманская область", "51");
        map.put("Нижегородская область", "52");
        map.put("Новгородская область", "53");
        map.put("Новосибирская область", "54");
        map.put("Омская область", "55");
        map.put("Оренбургская область", "56");
        map.put("Орловская область", "57");
        map.put("Пензенская область", "58");
        map.put("Пермский край", "59");
        map.put("Псковская область", "60");
        map.put("Ростовская область", "61");
        map.put("Рязанская область", "62");
        map.put("Самарская область", "63");
        map.put("Саратовская область", "64");
        map.put("Сахалинская область", "65");
        map.put("Свердловская область", "66");
        map.put("Смоленская область", "67");
        map.put("Тамбовская область", "68");
        map.put("Тверская область", "69");
        map.put("Томская область", "70");
        map.put("Тульская область", "71");
        map.put("Тюменская область", "72");
        map.put("Ульяновская область", "73");
        map.put("Челябинская область", "74");
        map.put("Забайкальский край", "75");
        map.put("Ярославская область", "76");
        map.put("г.Москва", "77");
        map.put("Санкт - Петербург", "78");
        map.put("Еврейская автономная область", "79");
        map.put("Ненецкий автономный округ", "83");
        map.put("Ханты - Мансийский автономный округ -Югра", "86");
        map.put("Чукотский автономный округ", "87");
        map.put("Ямало - Ненецкий автономный округ", "89");
        map.put("Иные территории, включая город и космодром Байконур", "99");
        String region = map.get(name);
        if (region == null) {
            System.out.println();
        }
        return region;
    }

}
