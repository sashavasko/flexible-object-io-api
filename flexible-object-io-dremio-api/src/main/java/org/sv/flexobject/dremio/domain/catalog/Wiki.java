package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.domain.ApiData;

public class Wiki extends ApiData {
    /**
     * Text displayed in the wiki, formatted with GitHub-flavored Markdown.
     * https://github.github.com/gfm/
     */
    public String text;
    /**
     * Number for the most recent version of the wiki, starting with 0.
     * Dremio increments the value by 1 each time the wiki changes and
     * uses the version value to ensure that updates apply to the
     * most recent version of the wiki.
     */
    public Integer version;
}
