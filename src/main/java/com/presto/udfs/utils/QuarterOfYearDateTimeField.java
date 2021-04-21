package com.presto.udfs.utils;

import org.joda.time.*;
import org.joda.time.field.DividedDateTimeField;
import org.joda.time.field.OffsetDateTimeField;
import org.joda.time.field.ScaledDurationField;

public final class QuarterOfYearDateTimeField
		extends DateTimeFieldType
{
	private static final long serialVersionUID = -5677872459807379123L;

	private static final DurationFieldType QUARTER_OF_YEAR_DURATION_FIELD_TYPE = new QuarterOfYearDurationFieldType();

	public static final DateTimeFieldType QUARTER_OF_YEAR = new QuarterOfYearDateTimeField();

	private QuarterOfYearDateTimeField()
	{
		super("quarterOfYear");
	}

	@Override
	public DurationFieldType getDurationType()
	{
		return QUARTER_OF_YEAR_DURATION_FIELD_TYPE;
	}

	@Override
	public DurationFieldType getRangeDurationType()
	{
		return DurationFieldType.years();
	}

	@Override
	public DateTimeField getField(Chronology chronology)
	{
		return new OffsetDateTimeField(new DividedDateTimeField(new OffsetDateTimeField(chronology.monthOfYear(), -1), QUARTER_OF_YEAR, 3), 1);
	}

	private static class QuarterOfYearDurationFieldType
			extends DurationFieldType
	{
		private static final long serialVersionUID = -8167713675442491871L;

		public QuarterOfYearDurationFieldType()
		{
			super("quarters");
		}

		@Override
		public DurationField getField(Chronology chronology)
		{
			return new ScaledDurationField(chronology.months(), QUARTER_OF_YEAR_DURATION_FIELD_TYPE, 3);
		}
	}
}

