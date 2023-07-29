package org.sv.flexobject.aws;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.properties.PropertiesWrapper;
import software.amazon.awssdk.regions.Region;

import java.util.Map;

public class CloudSecret extends Configured implements Tool  {

    public static class CloudSecretConfig extends PropertiesWrapper<CloudSecretConfig> {
        String key;
        String level;
        String arn;
        String region;
        String team;
        String role;

        @Override
        public CloudSecretConfig setDefaults() {
            level = "production";
            region = Region.US_EAST_1.toString();
            team = "DEPARTMENT";
            role = "ROLE";
            return this;
        }

        public String getLevel() {
            return level;
        }

        public String getArn() {
            return arn;
        }
        public boolean hasArn() {
            return StringUtils.isNotBlank(arn);
        }

        public Region getRegion() {
            return Region.of(region);
        }

        public boolean hasKey() {
            return StringUtils.isNotBlank(key);
        }
        public String getKey() {
            return key;
        }

        public String getTeam() {
            return team;
        }

        public String getRole() {
            return role;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "help", true, "help");
        options.addOption("k", "key", true, "key for secret in Vault");
        options.addOption("l", "level", true, "level for secret : development/staging/production (default is production)");
        options.addOption("t", "team", true, "team for secret");
        options.addOption("r", "role", true, "role for secret");
        options.addOption("a", "arn", true, "arn for secret to be displayed (if not specified then only AWS credentials will be printed)");
        options.addOption("R", "region", true, "aws region (default is US_EAST_1)");

        CommandLine cmd = new DefaultParser().parse(options, args, true);

        AWSSecretProviderWithVault.Builder providerBuilder = AWSSecretProviderWithVault.builder();
        CloudSecretConfig config = new CloudSecretConfig();
        if (!cmd.hasOption("help")) {
            config.from(cmd);

            if (config.hasKey()) {
                providerBuilder.key(config.getKey());
            } else {
                providerBuilder.team(config.getTeam());
                providerBuilder.role(config.getRole());
            }
        } else {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.setWidth(120);
            helpFormatter.printHelp("hdpjob [-jar <path_to_jar>] -drv org.sv.flexobject.aws.CloudSecret <job_options>\n", options);
            System.exit(1);
        }

        try(AWSSecretProviderWithVault provider = providerBuilder.build()) {
            if (!config.hasArn()) {
                Map<String, String> vaultCreds = provider.getAWSCredentialsFromVault(config.getLevel());
                System.out.println("\n");
                System.out.println("export AWS_ACCESS_KEY_ID=" + vaultCreds.get(AWSSecretProviderWithVault.ACCESS_KEY));
                System.out.println("export AWS_SECRET_ACCESS_KEY=" + vaultCreds.get(AWSSecretProviderWithVault.SECRET_KEY));
                System.out.println("export AWS_SESSION_TOKEN=" + vaultCreds.get(AWSSecretProviderWithVault.SECURITY_TOKEN));
                System.out.println("\n");
            } else {
                String secretString = provider.getAWSSecret(config.getArn(), config.getRegion(), config.getLevel());
                System.out.println("\n" + MapperFactory.pretty(secretString) + "\n");
            }
        }
        return 0;
    }
}
