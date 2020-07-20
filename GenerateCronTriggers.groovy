import org.apache.nifi.expression.ExpressionLanguageScope
import org.quartz.CronExpression
import java.util.Date

class GenerateCronTriggers implements Processor {

    public static final PropertyDescriptor STARTEPOCH = new PropertyDescriptor.Builder()
    .displayName("Start Epoch")
    .name("start-epoch")
    .description("The starting epoch in milliseconds upon which this comparison will be made.  Defaults to now.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .build()
    public static final PropertyDescriptor ENDEPOCH = new PropertyDescriptor.Builder()
    .displayName("End Epoch")
    .name("end-epoch")
    .description("The end epoch in milliseconds upon which this comparison will be made.  Defaults to now.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .build()
    public static final PropertyDescriptor CRONEXPRESSION = new PropertyDescriptor.Builder()
    .displayName("Cron Expression")
    .name("cron-expression")
    .description("The cron expression to be used in generating events that would have triggered between Start Epoch and End Epoch")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .build()
            
    def REL_ORIGINAL = new Relationship.Builder().name("original").description('FlowFiles that were successfully processed are routed here').build()
    def REL_TRIGGER = new Relationship.Builder().name("trigger").description('Flowfiles containing triggers between the specified epochs are routed here').build()
    def REL_FAILURE = new Relationship.Builder().name("failure").description('FlowFiles are routed here if an error occurs during processing').build()
    def log
    
    @Override
    void initialize(ProcessorInitializationContext context) {
        log = context.getLogger()
    }
    
    @Override    Set<Relationship> getRelationships() {
        return [REL_ORIGINAL,REL_TRIGGER,REL_FAILURE] as Set
    }
    
    @Override
    void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        String now = System.currentTimeMillis();
        def session = sessionFactory.createSession()
        def flowFile = session.get()
        try {
            if (!flowFile) return
            def startEpoch = context.getProperty(STARTEPOCH).evaluateAttributeExpressions(flowFile).getValue()
            if (!startEpoch) startEpoch = now
            def endEpoch = context.getProperty(ENDEPOCH).evaluateAttributeExpressions(flowFile).getValue()
            if(!endEpoch) endEpoch = now
            long startEpochLong = Long.valueOf(startEpoch);
            long endEpochLong = Long.valueOf(endEpoch);
            long nowLong = Long.valueOf(now);
            def cronExpressionEvaluated = context.getProperty(CRONEXPRESSION).evaluateAttributeExpressions(flowFile).getValue()
            final CronExpression cronExpression;
            cronExpression = new CronExpression(cronExpressionEvaluated);
            flowFile = session.putAttribute(flowFile, "start.epoch", startEpoch)
            flowFile = session.putAttribute(flowFile, "end.epoch", endEpoch)
            flowFile = session.putAttribute(flowFile, "cron.expression", cronExpressionEvaluated)
            Date next = cronExpression.getNextValidTimeAfter(new Date(startEpochLong))
            while(next.getTime() < endEpochLong) {
                FlowFile cloneFlowFile = session.clone(flowFile)
                cloneFlowFile = session.putAttribute(cloneFlowFile, "execute.epoch", (String)next.getTime())
                session.transfer(cloneFlowFile, REL_TRIGGER)
                next = cronExpression.getNextValidTimeAfter(next)
            }
            session.transfer(flowFile, REL_ORIGINAL)
            session.commit()
        }
        catch (e) {
            session.transfer(flowFile,REL_FAILURE)
            session.commit()
            throw new ProcessException(e)
        }
    }
    
    @Override
    Collection<ValidationResult> validate(ValidationContext context) { return null }
    
    @Override
    PropertyDescriptor getPropertyDescriptor(String name) { return null }
    
    @Override
    void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) { }
    
    @Override
    List<PropertyDescriptor> getPropertyDescriptors() {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(STARTEPOCH);
        descriptors.add(ENDEPOCH);
        descriptors.add(CRONEXPRESSION);
        return descriptors;
    }
    
    @Override
    String getIdentifier() { return null }
    
}

processor = new GenerateCronTriggers()
