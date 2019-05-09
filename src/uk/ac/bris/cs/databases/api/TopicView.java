package uk.ac.bris.cs.databases.api;

import java.util.List;
import uk.ac.bris.cs.databases.util.Params;

/**
 * Detailed view of a single topic (i.e. the posts).
 * @author csxdb
 */
public class TopicView {
    
    /* forumId and topicId identify this topic. */
    private final int forumId;
    private final int topicId;
    
    /* The name of the forum containing this topic. */
    private final String forumName;
    
    /* The title of this topic. */
    private final String title;
    
    /* The posts in this topic, in the order that they were created. */
    private final List<PostView> posts;
    
    public TopicView(int forumId, int topicId, String forumName, String title,
            List<PostView> posts) {
        
        Params.cannotBeEmpty(forumName);
        Params.cannotBeEmpty(title);
        Params.cannotBeEmpty(posts);
        
        this.forumId = forumId;
        this.topicId = topicId;
        this.forumName = forumName;
        this.title = title;
        this.posts = posts;
    }

    public List<PostView> getPosts() {
        return posts;
    }
    
    /**
     * @return the forumId
     */
    public int getForumId() {
        return forumId;
    }

    /**
     * @return the topicId
     */
    public int getTopicId() {
        return topicId;
    }

    /**
     * @return the forumName
     */
    public String getForumName() {
        return forumName;
    }

    /**
     * @return the title
     */
    public String getTitle() {
        return title;
    }
}
