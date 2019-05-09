package uk.ac.bris.cs.databases.web;

import uk.ac.bris.cs.databases.api.APIProvider;
import uk.ac.bris.cs.databases.api.Result;
import uk.ac.bris.cs.databases.api.SimpleTopicView;

/**
 *
 * @author csxdb
 */
public class SimpleTopicHandler extends SimpleHandler {

    @Override
    public RenderPair simpleRender(String p) {
        
        int id = Integer.parseInt(p);
        APIProvider api = ApplicationContext.getInstance().getApi();
        Result<SimpleTopicView> r = api.getSimpleTopic(id);
        return new RenderPair("SimpleTopicView.ftl", r);
    }
}
