package vk;

import com.vk.api.sdk.client.TransportClient;
import com.vk.api.sdk.client.VkApiClient;
import com.vk.api.sdk.client.actors.Actor;
import com.vk.api.sdk.client.actors.UserActor;
import com.vk.api.sdk.exceptions.ApiException;
import com.vk.api.sdk.exceptions.ClientException;
import com.vk.api.sdk.httpclient.HttpTransportClient;
import com.vk.api.sdk.objects.groups.GroupFull;
import com.vk.api.sdk.objects.groups.responses.GetMembersResponse;
import com.vk.api.sdk.objects.groups.responses.GetResponse;
import com.vk.api.sdk.objects.users.UserXtrCounters;
import com.vk.api.sdk.queries.groups.GroupField;
import com.vk.api.sdk.queries.groups.GroupsGetMembersQuery;
import com.vk.api.sdk.queries.users.UserField;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Test2 {

    public static final String filePath = "/home/vadim/answer.txt";

    public static final int APP_ID = 5642880;
    public static final String CLIENT_SECRET = "";

    static int userId = 386278042;
    static String accessToken = "";
    public static final String CODE = "";

    public static void main(String[] args) throws ClientException, ApiException, IOException, InterruptedException {
        start();
    }

    static void start() throws ClientException, ApiException, IOException {
        TransportClient transportClient = new HttpTransportClient();
        VkApiClient vk = new VkApiClient(transportClient);
/*
        AuthResponse response = vk.oauth()
                .userAuthorizationCodeFlow(APP_ID, CLIENT_SECRET, VkApi.REDIRECT_URI, CODE)
                .execute();
                System.out.println(String.format("userId: %s, accessToken: %s", response.getUserId(), response.getAccessToken()));
                */

        UserActor actor = new UserActor(userId, accessToken);

/*        List<UserXtrCounters> execute = vk.users().get(actor).execute();
        execute.stream().forEach(u ->
                        System.out.println(u.getFirstName())
        );*/

//        getAndSaveContacts(vk, actor);


//        collectContactsInfo(vk, "/home/vadim/test_data_for_vk.txt");
        collectContactsInfo(vk, filePath);



    }


    static void collectContactsInfo(VkApiClient vk, String path) throws IOException, ClientException, ApiException {
        List<String> strings = Files.readAllLines(Paths.get(path));

        LocalDate now = LocalDate.now();
        Pattern pattern = Pattern.compile("\\.");
        for (int from = 0; from < strings.size(); from += 1000) {
            int to = from + 1000 <= strings.size() ? from + 1000 : strings.size();
            String[] usersIds = strings.subList(from, to).stream().toArray(String[]::new);
            List<String> users = vk.users().get()
                    .userIds(usersIds)
                    .fields(UserField.SEX, UserField.BDATE, UserField.INTERESTS)
                    .execute()
                    .stream()
                    .map(u -> {
                        int age = -1;
                        if (u.getBdate() != null) {
                            String[] splits = pattern.split(u.getBdate());
                            if (splits.length == 3) {
                                LocalDate date = LocalDate.of(Integer.parseInt(splits[2]), Integer.parseInt(splits[1]), Integer.parseInt(splits[0]));
                                age = Period.between(date, now).getYears();
                            }
                        }
                        String interests = u.getInterests() == null || "".equals(u.getInterests()) ? "-1" : u.getInterests().replace("\n", " ");
                        return u.getSex() + "," + age + "," + interests;
                    })
                    .collect(Collectors.toList());
//            System.out.println("users string: " + users);

            Files.write(Paths.get("/home/vadim/contacts_vk_original3.csv"), users, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    static void getAndSaveContacts(VkApiClient vk, Actor actor) throws IOException, InterruptedException, ClientException, ApiException {
        GetResponse execute1 = vk.groups().get(actor).execute();
        Integer tedGroupId = execute1.getItems().get(0);

        List<GroupFull> execute2 = vk.groups()
                .getById()
                .groupId(String.valueOf(tedGroupId))
                .fields(GroupField.MEMBERS_COUNT)
                .execute();
        Integer membersCount = execute2.get(0).getMembersCount();

        execute2.stream().forEach(gf -> {
            System.out.println("group name: " + gf.getName());
            System.out.println("members count: " + gf.getMembersCount());
            System.out.println("users: ");
        });

        List<Integer> all = new ArrayList<>(membersCount);
        List<Long> times = new ArrayList<>();
        for (int offset = 0; all.size() < membersCount; offset = all.size()) {
            List<Integer> contacts = new ArrayList<>();
            try {
                times.add(System.currentTimeMillis());
                contacts = a(vk.groups().getMembers(), tedGroupId, offset);
                times.add(System.currentTimeMillis());
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println(e.toString());
                System.out.println(times);
                Thread.sleep(1000L);
            }
            all.addAll(contacts);
        }

//        System.out.println("times: " + times);
        Files.write(Paths.get("/home/vadim/answer.txt"),
                all.stream().map(String::valueOf).collect(Collectors.toList()), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    static List<Integer> a(GroupsGetMembersQuery query, int groupId, int offset) throws ClientException, ApiException {
        GetMembersResponse response = query
                .groupId(String.valueOf(groupId))
                .offset(offset)
                .execute();
        System.out.println("size list: " + response.getItems().size());
        return response.getItems();
    }
}
