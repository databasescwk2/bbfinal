/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.bris.cs.databases.cwk2;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import uk.ac.bris.cs.databases.api.Result;
import uk.ac.bris.cs.databases.api.SimplePostView;
import uk.ac.bris.cs.databases.api.SimpleTopicSummaryView;

/**
 *
 * @author heman
 */
public class ViewGetter {
    private final Connection c;

    public ViewGetter(Connection c) {
        this.c = c;
    }

    Result<SimpleTopicSummaryView> lastSimpleTopic(int forumId){
        final String stmt = "SELECT Topic.id as topicId, Forum.id as forumId, Topic.title " +
                            "FROM Topic INNER JOIN Forum ON forumId=Forum.id " +
                            "WHERE forumId = ? " +
                            "ORDER BY Topic.id DESC LIMIT 1";
        SimpleTopicSummaryView lastTopic;
        try(PreparedStatement s = c.prepareStatement(stmt)){
            s.setInt(1, forumId);
            ResultSet r = s.executeQuery();
            if(r.next() == false){
                return Result.success(null);
            }
            lastTopic = new SimpleTopicSummaryView(r.getInt("topicId"),forumId, r.getString("title"));
            return Result.success(lastTopic);
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    Result<List<SimpleTopicSummaryView>> topicList(int forumId){
        final String stmt = "SELECT Topic.id as topicId, Forum.id as forumId, Topic.title " +
                "FROM Topic INNER JOIN Forum ON forumId=Forum.id " +
                "WHERE forumId = ? " +
                "ORDER BY Topic.id DESC";
        SimpleTopicSummaryView Topic;
        List<SimpleTopicSummaryView> tl = new ArrayList<>();
        try(PreparedStatement s = c.prepareStatement(stmt)){
            s.setInt(1, forumId);
            ResultSet r = s.executeQuery();
            while(r.next()){
                Topic = new SimpleTopicSummaryView(r.getInt("topicId"),forumId, r.getString("title"));
                tl.add(Topic);
            }
            System.out.println(tl.size());
            return Result.success(tl);
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    Result<List<SimplePostView>> postList(int topicId){
        final String stmt = "SELECT * FROM Post WHERE topicId = ?";
        List<SimplePostView> pl = new ArrayList<>();
        try(PreparedStatement s = c.prepareStatement(stmt)){
            s.setInt(1, topicId);
            ResultSet r = s.executeQuery();
            while(r.next()){
                Integer postNo = r.getInt("postNumber");
                Integer authorId = r.getInt("authorId");
                Result resName = getUsername(authorId);
                if (!resName.isSuccess()) {return resName;}
                String text = r.getString("text");
                String postedAt = r.getDate("postedAt").toString();
                pl.add(new SimplePostView(postNo, (String) resName.getValue(),
                                            text, postedAt));
            }
            return Result.success(pl);
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    Result<Integer> getUserId(String username){
        System.out.println("userId Problem");
        final String stmt = "SELECT * FROM Person WHERE username = ?";
        System.out.println("userId Problem");
        try(PreparedStatement s = c.prepareStatement(stmt)){
            s.setString(1, username);
            ResultSet r = s.executeQuery();
            System.out.println("userId Problem");
            if (r.next() == false){
                return Result.failure("getUserId: person does not exist");
            }
            return Result.success(r.getInt("id"));
        } catch (SQLException e) {
            System.out.println("userId Problem");
            return Result.fatal(e.getMessage());
        }
    }

    Result<String> getUsername(int id){
        final String stmt = "SELECT * FROM Person WHERE id = ?";
        try(PreparedStatement s = c.prepareStatement(stmt)){
            s.setInt(1, id);
            ResultSet r = s.executeQuery();
            if (r.next() == false){
                return Result.failure("getUsername: person does not exist");
            }
            return Result.success(r.getString("username"));
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    Result incrementPostCount(int topicId){
        final String stmt = "UPDATE Topic " +
                            "SET postCount = postCount + 1 " +
                            "WHERE id = ?";
        try(PreparedStatement s = c.prepareStatement(stmt)){
            s.setInt(1, topicId);
            if(s.executeUpdate() != 1){
                try{ c.rollback(); }
                catch (SQLException f){ return Result.fatal(f.getMessage()); }
                return Result.failure("incrementPostCount: unexpected number of rows updated");
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

    Result<Integer> getPersonId(String username) {
        final String stmt = "SELECT * FROM Person WHERE username = ?";
        try(PreparedStatement s = c.prepareStatement(stmt)){
            s.setString(1, username);
            ResultSet r = s.executeQuery();
            if (r.next() == false) { Result.failure("getPersonId: Person does not exist"); }
            return Result.success(r.getInt("id"));
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    java.sql.Date getDateTime(){
        java.util.Date today = new java.util.Date();
        return new java.sql.Date(today.getTime());
    }
}
