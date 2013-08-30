package com.pearson.entech.elasticsearch.search.facet.approx.date;

import java.io.IOException;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.joda.time.Chronology;
import org.elasticsearch.common.joda.time.DateTimeField;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.joda.time.MutableDateTime;
import org.elasticsearch.common.joda.time.chrono.ISOChronology;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.trove.impl.Constants;
import org.elasticsearch.common.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.FacetParser;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Parser is responsible to make sense of a SearchRequests "facet query" and
 * has to choose the correct FacetExecutor based on the facet query
 *
 * The {@link #parse} method does all the interesting work.
 */
public class DateFacetParser extends AbstractComponent implements FacetParser {

    private final ImmutableMap<String, DateFieldParser> dateFieldParsers;
    private final TObjectIntHashMap<String> rounding = new TObjectIntHashMap<String>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1);

    @Inject
    public DateFacetParser(final Settings settings) {
        super(settings);

        dateFieldParsers = MapBuilder.<String, DateFieldParser> newMapBuilder()
                .put("year", new DateFieldParser.YearOfCentury())
                .put("1y", new DateFieldParser.YearOfCentury())
                .put("quarter", new DateFieldParser.Quarter())
                .put("month", new DateFieldParser.MonthOfYear())
                .put("1m", new DateFieldParser.MonthOfYear())
                .put("week", new DateFieldParser.WeekOfWeekyear())
                .put("1w", new DateFieldParser.WeekOfWeekyear())
                .put("day", new DateFieldParser.DayOfMonth())
                .put("1d", new DateFieldParser.DayOfMonth())
                .put("hour", new DateFieldParser.HourOfDay())
                .put("1h", new DateFieldParser.HourOfDay())
                .put("minute", new DateFieldParser.MinuteOfHour())
                .put("1m", new DateFieldParser.MinuteOfHour())
                .put("second", new DateFieldParser.SecondOfMinute())
                .put("1s", new DateFieldParser.SecondOfMinute())
                .immutableMap();

        rounding.put("floor", MutableDateTime.ROUND_FLOOR);
        rounding.put("ceiling", MutableDateTime.ROUND_CEILING);
        rounding.put("half_even", MutableDateTime.ROUND_HALF_EVEN);
        rounding.put("halfEven", MutableDateTime.ROUND_HALF_EVEN);
        rounding.put("half_floor", MutableDateTime.ROUND_HALF_FLOOR);
        rounding.put("halfFloor", MutableDateTime.ROUND_HALF_FLOOR);
        rounding.put("half_ceiling", MutableDateTime.ROUND_HALF_CEILING);
        rounding.put("halfCeiling", MutableDateTime.ROUND_HALF_CEILING);
    }

    @Override
    public String[] types() {
        return new String[] {
                "date_facet"
        };
    }

    @Override
    public FacetExecutor.Mode defaultMainMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public FacetExecutor.Mode defaultGlobalMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public FacetExecutor parse(final String facetName, final XContentParser parser, final SearchContext context) throws IOException {
        String keyField = null;
        String distinctField = null;
        String valueField = null;
        String sliceField = null;
        //        final String valueScript = null;
        //        String scriptLang = null;
        //        Map<String, Object> params = null;
        String interval = null;
        DateTimeZone preZone = DateTimeZone.UTC;
        DateTimeZone postZone = DateTimeZone.UTC;
        boolean preZoneAdjustLargeInterval = false;
        long preOffset = 0;
        long postOffset = 0;
        float factor = 1.0f;
        final Chronology chronology = ISOChronology.getInstanceUTC();
        XContentParser.Token token;
        String fieldName = null;
        int exactThreshold = 1000;

        while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if(token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if(token == XContentParser.Token.START_OBJECT) {
                //                if("params".equals(fieldName)) {
                //                    params = parser.map();
                //                }
            } else if(token.isValue()) {
                if("field".equals(fieldName)) {
                    keyField = parser.text();
                } else if("key_field".equals(fieldName) || "keyField".equals(fieldName)) {
                    keyField = parser.text();
                } else if("value_field".equals(fieldName) || "valueField".equals(fieldName)) {
                    valueField = parser.text();
                } else if("distinct_field".equals(fieldName) || "distinctField".equals(fieldName)) {
                    distinctField = parser.text();
                } else if("slice_field".equals(fieldName) || "sliceField".equals(fieldName)) {
                    sliceField = parser.text();
                } else if("interval".equals(fieldName)) {
                    interval = parser.text();
                } else if("time_zone".equals(fieldName) || "timeZone".equals(fieldName)) {
                    preZone = parseZone(parser, token);
                } else if("pre_zone".equals(fieldName) || "preZone".equals(fieldName)) {
                    preZone = parseZone(parser, token);
                } else if("pre_zone_adjust_large_interval".equals(fieldName) || "preZoneAdjustLargeInterval".equals(fieldName)) {
                    preZoneAdjustLargeInterval = parser.booleanValue();
                } else if("post_zone".equals(fieldName) || "postZone".equals(fieldName)) {
                    postZone = parseZone(parser, token);
                } else if("pre_offset".equals(fieldName) || "preOffset".equals(fieldName)) {
                    preOffset = parseOffset(parser.text());
                } else if("post_offset".equals(fieldName) || "postOffset".equals(fieldName)) {
                    postOffset = parseOffset(parser.text());
                } else if("factor".equals(fieldName)) {
                    factor = parser.floatValue();
                    /*
                    } else if("value_script".equals(fieldName) || "valueScript".equals(fieldName)) {
                    valueScript = parser.text();
                    */
                    //                } else if("lang".equals(fieldName)) {
                    //                    scriptLang = parser.text();
                } else if("exact_threshold".equals(fieldName) || "exactThreshold".equals(fieldName)) {
                    exactThreshold = parser.intValue();
                }
            }
        }

        if(valueField != null && distinctField != null)
            throw new FacetPhaseExecutionException(facetName, "[value_field] and [distinct_field] may not be used together");

        if(interval == null) {
            throw new FacetPhaseExecutionException(facetName, "[interval] is required to be set for histogram facet");
        }

        TimeZoneRounding.Builder tzRoundingBuilder;
        final DateFieldParser fieldParser = dateFieldParsers.get(interval);
        if(fieldParser != null) {
            tzRoundingBuilder = TimeZoneRounding.builder(fieldParser.parse(chronology));
        } else {
            // the interval is a time value?
            tzRoundingBuilder = TimeZoneRounding.builder(TimeValue.parseTimeValue(interval, null));
        }

        final TimeZoneRounding tzRounding = tzRoundingBuilder
                .preZone(preZone).postZone(postZone)
                .preZoneAdjustLargeInterval(preZoneAdjustLargeInterval)
                .preOffset(preOffset).postOffset(postOffset)
                .factor(factor)
                .build();

        final TypedFieldData keyFieldData = getFieldData(keyField, context);
        if(keyFieldData == null)
            throw new FacetPhaseExecutionException(facetName, "[key_field] is required to be set for distinct date histogram facet");
        if(!"long".equals(keyFieldData.type.getType()))
            throw new FacetPhaseExecutionException(facetName, "key field [" + keyField + "] is not of type date");

        final TypedFieldData valueFieldData = getFieldData(valueField, context);
        final TypedFieldData distinctFieldData = getFieldData(distinctField, context);
        final TypedFieldData sliceFieldData = getFieldData(sliceField, context);

        if(exactThreshold < 0)
            exactThreshold = Integer.MAX_VALUE;

        final boolean debug = false;

        return new DateFacetExecutor(keyFieldData, valueFieldData, distinctFieldData, sliceFieldData,
                tzRounding, exactThreshold, debug);

        // TODO implement scripts
    }

    private TypedFieldData getFieldData(final String fieldName, final SearchContext context) {
        if(fieldName != null) {
            final FieldMapper<?> mapper = context.smartNameFieldMapper(fieldName);
            if(mapper == null) {
                throw new FacetPhaseExecutionException(fieldName, "no mapping found for " + fieldName);
            }
            final FieldDataType fieldDataType = mapper.fieldDataType();
            final IndexFieldData fieldData = context.fieldData().getForField(mapper);
            return new TypedFieldData(fieldData, fieldDataType);
        }
        return null;
    }

    private long parseOffset(final String offset) throws IOException {
        if(offset.charAt(0) == '-') {
            return -TimeValue.parseTimeValue(offset.substring(1), null).millis();
        }
        final int beginIndex = offset.charAt(0) == '+' ? 1 : 0;
        return TimeValue.parseTimeValue(offset.substring(beginIndex), null).millis();
    }

    private DateTimeZone parseZone(final XContentParser parser, final XContentParser.Token token) throws IOException {
        if(token == XContentParser.Token.VALUE_NUMBER) {
            return DateTimeZone.forOffsetHours(parser.intValue());
        } else {
            final String text = parser.text();
            final int index = text.indexOf(':');
            if(index != -1) {
                final int beginIndex = text.charAt(0) == '+' ? 1 : 0;
                // format like -02:30
                return DateTimeZone.forOffsetHoursMinutes(
                        Integer.parseInt(text.substring(beginIndex, index)),
                        Integer.parseInt(text.substring(index + 1))
                        );
            } else {
                // id, listed here: http://joda-time.sourceforge.net/timezones.html
                return DateTimeZone.forID(text);
            }
        }
    }

    private boolean isIntegral(final TypedFieldData typedFieldData) {
        if(typedFieldData == null)
            return false;
        final String typeName = typedFieldData.type.getType();
        return "byte".equals(typeName)
                || "short".equals(typeName)
                || "int".equals(typeName)
                || "long".equals(typeName);
    }

    private boolean isString(final TypedFieldData typedFieldData) {
        if(typedFieldData == null)
            return false;
        final String typeName = typedFieldData.type.getType();
        return "string".equals(typeName);
    }

    static interface DateFieldParser {

        // Nothing special here; 1:1 the same as in the DateHistogramFacetParser

        DateTimeField parse(Chronology chronology);

        static class WeekOfWeekyear implements DateFieldParser {
            @Override
            public DateTimeField parse(final Chronology chronology) {
                return chronology.weekOfWeekyear();
            }
        }

        static class YearOfCentury implements DateFieldParser {
            @Override
            public DateTimeField parse(final Chronology chronology) {
                return chronology.yearOfCentury();
            }
        }

        static class Quarter implements DateFieldParser {
            @Override
            public DateTimeField parse(final Chronology chronology) {
                return Joda.QuarterOfYear.getField(chronology);
            }
        }

        static class MonthOfYear implements DateFieldParser {
            @Override
            public DateTimeField parse(final Chronology chronology) {
                return chronology.monthOfYear();
            }
        }

        static class DayOfMonth implements DateFieldParser {
            @Override
            public DateTimeField parse(final Chronology chronology) {
                return chronology.dayOfMonth();
            }
        }

        static class HourOfDay implements DateFieldParser {
            @Override
            public DateTimeField parse(final Chronology chronology) {
                return chronology.hourOfDay();
            }
        }

        static class MinuteOfHour implements DateFieldParser {
            @Override
            public DateTimeField parse(final Chronology chronology) {
                return chronology.minuteOfHour();
            }
        }

        static class SecondOfMinute implements DateFieldParser {
            @Override
            public DateTimeField parse(final Chronology chronology) {
                return chronology.secondOfMinute();
            }
        }
    }
}
