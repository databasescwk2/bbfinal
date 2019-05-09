package uk.ac.bris.cs.databases.cwk2;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import uk.ac.bris.cs.databases.api.APIProvider;
import uk.ac.bris.cs.databases.api.AdvancedForumSummaryView;
import uk.ac.bris.cs.databases.api.AdvancedForumView;
import uk.ac.bris.cs.databases.api.ForumSummaryView;
import uk.ac.bris.cs.databases.api.ForumView;
import uk.ac.bris.cs.databases.api.AdvancedPersonView;
import uk.ac.bris.cs.databases.api.PostView;
import uk.ac.bris.cs.databases.api.Result;
import uk.ac.bris.cs.databases.api.PersonView;
import uk.ac.bris.cs.databases.api.SimpleForumSummaryView;
import uk.ac.bris.cs.databases.api.SimplePostView;
import uk.ac.bris.cs.databases.api.SimpleTopicSummaryView;
import uk.ac.bris.cs.databases.api.SimpleTopicView;
import uk.ac.bris.cs.databases.api.TopicView;

/**
 *
 * @author csxdb
 */
public class API implements APIProvider {

    private final Connection c;
    private final ViewGetter sg;

    public API(Connection c) {
        this.c = c;
        sg = new ViewGetter(c);
    }

    /* A.1 */

    @Override
    public Result<Map<String, String>> getUsers() {
        final String stmt = "SELECT username, name FROM Person;";
        Map<String, String> usermap = new LinkedHashMap<>();
        try(PreparedStatement s = c.prepareStatement(stmt)){
            ResultSet r = s.executeQuery();
            while(r.next()){
                usermap.put(r.getString("username"), r.getString("name"));
            }
            return Result.success(usermap);
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result<PersonView> getPersonView(String username) {
        final String stmt = "SELECT name, username, stuId FROM Person WHERE username = ?";
        String name, studentID;
        if (username == null || username.isEmpty()){
            return Result.failure("getPersonView: username may not be empty");
        }
        try(PreparedStatement s = c.prepareStatement(stmt)){
            s.setString(1,username);
            ResultSet r = s.executeQuery();
            if (r.next()){
                name = r.getString("name");
                studentID = r.getString("stuId");
            }
            else{
                return Result.failure("getPersonView: Specified username does not exist");
            }
            PersonView pv;
            if (studentID != null) { pv = new PersonView(name, username, studentID); }
            else { pv = new PersonView(name, username, ""); }
            return Result.success(pv);
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result addNewPerson(String name, String username, String studentId) {
        String stmt = "INSERT INTO Person (name, username, stuId)" +
                            "VALUES (?, ?, ?)";
        final String nullstmt = "INSERT INTO Person (name, username)" +
                            "VALUES (?, ?)";
        if (name == null || username == null || name.isEmpty() || username.isEmpty()){
            return Result.failure("addNewPerson: name and username must be filled");
        }
        if (studentId == null){ stmt = nullstmt; }
        try(PreparedStatement s = c.prepareStatement(stmt)){
            s.setString(1, name);
            s.setString(2, username);
            if (studentId != null){ s.setString(3, studentId); }
            if(s.executeUpdate() == 0){
                return Result.failure("addNewPerson: username already exists");
            }
            c.commit();
            return Result.success();
        } catch (SQLException e) {
            try{
                c.rollback();
            }
            catch (SQLException f){
                return Result.fatal(f.getMessage());
            }
            return Result.fatal(e.getMessage());
        }
    }

    /* A.2 */

    @Override
    public Result<List<SimpleForumSummaryView>> getSimpleForums() {
        final String stmt = "SELECT id, title FROM Forum";
        List<SimpleForumSummaryView> flist = new ArrayList<>();
        try(PreparedStatement s = c.prepareStatement(stmt)){
            ResultSet r = s.executeQuery();
            while(r.next()){
                flist.add(new SimpleForumSummaryView(r.getInt("id"), r.getString("title")));
            }
            return Result.success(flist);
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result createForum(String title) {
        String stmt = "INSERT INTO Forum (title)" +
                "VALUES (?)";
        if (title == null || title.isEmpty()){
            return Result.failure("createForum: title must be filled");
        }
        try(PreparedStatement s = c.prepareStatement(stmt)){
            s.setString(1, title);
            if(s.executeUpdate() == 0){
                return Result.failure("createForum: A forum with this title already exists");
            }
            c.commit();
            return Result.success();
        } catch (SQLException e) {
            try{
                c.rollback();
            }
            catch (SQLException f){
                return Result.fatal(f.getMessage());
            }
            return Result.fatal(e.getMessage());
        }
    }

    /* A.3 */

    @Override
    public Result<List<ForumSummaryView>> getForums() {
        final String stmt = "SELECT id, title FROM Forum ORDER BY title ASC";
        List<ForumSummaryView> forumlist = new ArrayList<>();
        try(PreparedStatement s = c.prepareStatement(stmt)){
            ResultSet r = s.executeQuery();
            while(r.next()){
                Integer id = r.getInt("id");
                String title = r.getString("title");
                Result res = sg.lastSimpleTopic(id);
                if (!res.isSuccess()){ return res; }
                SimpleTopicSummaryView lastPost = (SimpleTopicSummaryView) res.getValue();
                forumlist.add(new ForumSummaryView(id, title, lastPost));
            }
            return Result.success(forumlist);
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result<ForumView> getForum(int id) {
        final String stmt = "SELECT id, title FROM Forum WHERE id = ?";
        try(PreparedStatement s = c.prepareStatement(stmt)){
            s.setInt(1,id);
            ResultSet r = s.executeQuery();
            if (r.next() == false){
                return Result.failure("getForum: No forum exists with the given id");
            }
            Integer fId = r.getInt("id");
            String title = r.getString("title");
            Result res = sg.topicList(fId);
            if (!res.isSuccess()){ return res; }
            List<SimpleTopicSummaryView> topics = (List<SimpleTopicSummaryView>) res.getValue();
            return Result.success(new ForumView(fId, title,  topics));
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result<SimpleTopicView> getSimpleTopic(int topicId) {
        final String stmt = "SELECT id, Topic.title as title FROM Topic " +
                            "WHERE id = ?";
        SimpleTopicView st;
        try(PreparedStatement s = c.prepareStatement(stmt)){
            s.setInt(1, topicId);
            ResultSet r = s.executeQuery();
            if (r.next() == false){
                return Result.failure("getSimpleTopic: No topic with this topic ID");
            }
            Result res = sg.postList(topicId);
            if (!res.isSuccess() && res.isFatal()){return Result.fatal(res.getMessage());}
            List<SimplePostView> pl = (List<SimplePostView>) res.getValue();
            System.out.println(pl.size());
            st = new SimpleTopicView(topicId, r.getString("title"), pl);
            return Result.success(st);
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result<PostView> getLatestPost(int topicId) {
        final String stmt = "SELECT * FROM Post INNER JOIN Person ON Person.id = authorId" +
                            "WHERE topicId = ? " +
                            "ORDER BY id DESC LIMIT 1";
        PostView lastPost;
        try(PreparedStatement s = c.prepareStatement(stmt)){
            s.setInt(1, topicId);
            ResultSet r = s.executeQuery();
            if(r.next() == false){
                return Result.failure("getLatestPost: No post exists");
            }
            lastPost = new PostView(r.getInt("forumId"), topicId, r.getInt("postNumber"),
                                    r.getString("Person.name"), r.getString("Person.username"),
                                    r.getString("text"), r.getDate("postedAt").toString(),
                                    r.getInt("likes"));
            return Result.success(lastPost);
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result createPost(int topicId, String username, String text) {
        String stmt = "INSERT INTO Post (topicId, postNumber, authorId," +
                        "text, postedAt, likes) VALUES(?,?,?,?,?,?)";
        if (text == null || text.isEmpty()){
            return Result.failure("createPost: text must be filled");
        }
        Result<Integer> numP = countPostsInTopic(topicId);
        if (!numP.isSuccess()) { return numP; }
        Integer numPosts = numP.getValue() + 1;
        Result<Integer> idRes = sg.getPersonId(username);
        if (!idRes.isSuccess()) {
            System.out.println("post creation failed at database no upfate"); return idRes; }
        try(PreparedStatement s = c.prepareStatement(stmt)){
            s.setInt(1, topicId);
            s.setInt(2, numPosts);
            s.setInt(3, idRes.getValue());
            s.setString(4, text);
            s.setDate(5, sg.getDateTime());
            s.setInt(6, 0);
            if (s.executeUpdate() != 1){
                return Result.failure("createPost: database did not update");
            }
            c.commit();
            sg.incrementPostCount(topicId);
            c.commit();
            return Result.success();
        } catch (SQLException e) {
            try{
                c.rollback();
            }
            catch (SQLException f){
                return Result.fatal(f.getMessage());
            }
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result createTopic(int forumId, String username, String title, String text) {
        final String stmt = "INSERT INTO Topic (forumId, title, postCount, likes) " +
                "VALUES(?,?,?,?)";

        try(PreparedStatement s = c.prepareStatement(stmt)){
            s.setInt(1, forumId);
            s.setString(2, title);
            s.setInt(3, 0);
            s.setInt(4, 0);
            if(s.executeUpdate() != 1){
                c.rollback();
                return Result.failure("createTopic: database did not update");
            }
            c.commit();
            System.out.println("created topic");
            ResultSet k =  s.getGeneratedKeys();
            int key = -1;
            if (k != null && k.next()) {
                key = k.getInt(1);
            }
            System.out.println(key);
            Result newpost = createPost(key, username, text);
            System.out.println("created post");
            if(!newpost.isSuccess()){
                System.out.println("post creation failed");
                return newpost;
            }
            c.commit();
            return Result.success();
        } catch (SQLException e) {
            try{
                c.rollback();
            }
            catch (SQLException f){
                return Result.fatal(f.getMessage());
            }
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result<Integer> countPostsInTopic(int topicId) {
        final String stmt = "SELECT postCount FROM Topic WHERE id = ?";
        try(PreparedStatement s = c.prepareStatement(stmt)){
            s.setInt(1, topicId);
            ResultSet r = s.executeQuery();
            if (r.next() == false){
                Result.failure("countPostsInTopic: post does not exist");
            }
            return Result.success(r.getInt("postCount"));
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    /* B.1 */

    @Override
    public Result likeTopic(String username, int topicId, boolean like) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result likePost(String username, int topicId, int post, boolean like) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<List<PersonView>> getLikers(int topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<TopicView> getTopic(int topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /* B.2 */

    @Override
    public Result<List<AdvancedForumSummaryView>> getAdvancedForums() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<AdvancedPersonView> getAdvancedPersonView(String username) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<AdvancedForumView> getAdvancedForum(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
