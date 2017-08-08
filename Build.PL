use Module::Build qw();
my $builder = Module::Build->new(
    module_name       => 'NGCP-Cleanup',
    license           => 'perl',
    dist_author       => 'Kirill Solomko <ksolomko@sipwise.com>',
    dist_version_from => 'lib/NGCP/Cleanup.pm',
    requires          => {
        'DateTime'          => 0,
    },
);
$builder->create_build_script;
