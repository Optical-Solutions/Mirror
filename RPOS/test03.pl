  my $data_dir = '/usr/local/mccs/data/rpos/archive/iri_pos_daily/';
  my $date_buffer = `ls -lar $data_dir`;
  my @ary = split(/\n/, $date_buffer);
    my $size = @ary;
    if($size < 10){
        print 'not enough files';
    }
    else{
        print 'we have enough files';
    }