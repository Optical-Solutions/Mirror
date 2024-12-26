package MCCS::Math::Retail;

use strict;
use warnings;

use Carp qw(croak);
use Exporter qw(import);
#our @EXPORT = qw(retail cost markup_dols markup_pct cost_complement markdown
#savings net_markups net_markdowns turnover gross_margin_pct
#gross_margin_dols retail_reductions cumulative_markon gmroi
#stock_shortage);


sub retail
{
   my %args = (
      cost => undef,
      markup => undef,
      cost_complement => undef,
      markup_dols => undef,
      markup_pct => undef
   );
     while (my $key = shift {
        my $value = shift;
        $args[$key} = $value;
        }
        my $ret_value = undef;
   # Retail($) = Cost($) + Markup($)
   if (defined $args{cost} && defined $args{markup})
   {
      $ret_value = $args{cost} + $args{markup};

   }
   # Retail($) = Cost($) / Cost Complement(%)
   elsif (defined $args{cost} && defined $args{cost_complement})
   {
      $ret_value = $args{cost} / $args{cost_complement};
   }
   # Retail($) = Markup($) / Markup(%)
   elsif (defined $args{markup_dols} && defined $args{markup_pct})
   {
      $ret_value = $args{markup_dols} / $args{markup_pct};
   }
   if (not(defined  $ret_value)){
      croak "No valid parameters passed.";
   }
   return $ret_value;

}


sub cost
{
   my %args = (
      retail => undef,
      markup => undef,
      cost_complement => undef,
      @_
   );

   # Cost($) = Retail($) - Markup($)
   if (defined $args{retail} && defined $args{markup})
   {
      return $args{retail} - $args{markup};
   }
   # Cost($) = Retail($) * Cost Complement(%)
   elsif (defined $args{retail} && defined $args{cost_complement})
   {
      return $args{retail} * $args{cost_complement};
   }
   else
   {
      croak "No valid parameters passed.";
   }
}


sub markup_dols
{
   my %args = (
      retail => undef,
      cost => undef,
      markup => undef,
      @_
   );

   # Markup($) = Retail($) - Cost($)
   if (defined $args{retail} && defined $args{cost})
   {
      return $args{retail} - $args{cost};
   }
   # Markup($) = Retail($) * Markup(%)
   elsif (defined $args{retail} && defined $args{markup})
   {
      return $args{retail} * $args{markup};
   }
   else
   {
      croak "No valid parameters passed.";
   }
}


sub markup_pct
{
   my %args = (
      retail => undef,
      cost => undef,
      markup => undef,
      cost_complement => undef,
      @_      
   );

   # Markup(%) = (Retail($) - Cost($)) / Retail($)
   if (defined $args{retail} && defined $args{cost})
   {
      return ($args{retail} - $args{cost}) / $args{retail};
   }
   # Markup(%) = Markup($) / Retail($)
   elsif (defined $args{markup} && defined $args{retail})
   {
      return $args{markup} / $args{retail};
   }
   # Markup(%) = 100% - Cost Complement(%)
   elsif (defined $args{cost_complement})
   {
      return 1 - $args{cost_complement};
   }
   else
   {
      croak "No valid parameters passed.";
   }
}


sub cost_complement
{
   my %args = (
      markup => undef,
      cost => undef,
      retail => undef,
      @_
   );

   # Cost Complement(%) = 100% - Markup(%)
   if (defined $args{markup})
   {
      return 1 - $args{markup};
   }
   # Cost Complement(%) = Cost($) / Retail($)
   elsif (defined $args{cost} && defined $args{retail})
   {
      return $args{cost} / $args{retail};
   }
   else
   {
      croak "No valid parameters passed.";
   }   
}


sub markdown
{
   my %args = (
      retail => undef,
      new_price => undef,
      net_markdown => undef,
      net_sales => undef,
      @_
   );

   # Markdown(%) = (Retail($) - New price($)) / New price($)
   if (defined $args{retail} && defined $args{new_price})
   {
      return ($args{retail} - $args{new_price}) / $args{new_price};
   }
   # Markdown(%) = Net Markdown($) / Net Sales($)
   elsif (defined $args{net_markdown} && defined $args{net_sales})
   {
      return $args{net_markdown} / $args{net_sales};
   }
   else
   {
      croak "No valid parameters passed.";
   }
}


sub savings
{
   my %args = (
      retail => undef,
      new_price => undef,
      @_
   );

   # Savings(%) = (Retail($) - New price($)) / Retail($)
   if (defined $args{retail} && defined $args{new_price})
   {
      return ($args{retail} - $args{new_price}) / $args{retail};
   }
   else
   {
      croak "No valid parameters passed.";
   }
}


sub net_markups
{
   my %args = (
      gross_markups => undef,
      markups_cancelled => undef,
      @_
   );

   # Net Markups($) = Gross Markups($) - Markup Cancellations($)
   if (defined $args{gross_markups} && defined $args{markups_cancelled})
   {
      return $args{gross_markups} - $args{markups_cancelled};
   }
   else
   {
      croak "No valid parameters passed.";
   }
}


sub net_markdowns
{
   my %args = (
      gross_markdowns => undef,
      markdowns_cancelled => undef,
      @_
   );

   # Net Markdowns($) = Gross Markdowns($) - Markdown Cancellations($)
   if (defined $args{gross_markdowns} && defined $args{markdowns_cancelled})
   {
      return $args{gross_markdowns} - $args{markdowns_cancelled};
   }
   else
   {
      croak "No valid parameters passed.";
   }
}


sub turnover
{
   my %args = (
      sales => undef,
      average_inventory_retail => undef,
      @_
   );

   # Turnover = Sales($) / Average Inventory at Retail($)
   if (defined $args{sales} && defined $args{average_inventory_retail})
   {
      return $args{sales} / $args{average_inventory_retail};
   }
   else
   {
      croak "No valid parameters passed.";
   }
}


sub gross_margin_pct
{
   my %args = (
      gross_margin => undef,
      net_sales => undef,
      cumulative_markon => undef,
      cost_complement => undef,
      retail_reductions => undef,
      @_
   );

   # Gross Margin(%) = Gross Margin($) / Net Sales(%)
   if (defined $args{gross_margin} && defined $args{net_sales})
   {
      return $args{gross_margin} / $args{net_sales};
   }
   # Gross Margin(%) = Cumulative Markon(%) -
   #                   (Cost Complement(%) * Retail Reductions(%))
   elsif (defined $args{cumulative_markon} && defined $args{cost_complement} &&
          defined $args{retail_reductions})
   {
      return $args{cumulative_markon} -
             ($args{cost_complement} * $args{retail_reductions});
   }
   else
   {
      croak "No valid parameters passed.";
   }
}


sub gross_margin_dols
{
   my %args = (
      net_sales => undef,
      gross_margin => undef,
      sold_goods_cost => undef,
      @_
   );

   # Gross Margin($) = Net Sales($) * Gross Margin(%)
   if (defined $args{net_sales} && defined $args{gross_margin})
   {
      return $args{net_sales} * $args{gross_margin};
   }
   # Gross Margin($) = Net Sales($) - Cost of Goods Sold($)
   elsif (defined $args{net_sales} && defined $args{sold_goods_cost})
   {
      return $args{net_sales} - $args{sold_goods_cost};
   }
   else
   {
      croak "No valid parameters passed.";
   }
}


sub retail_reductions
{
   my %args = (
      markdown => undef,
      employee_discount => undef,
      shrinkage => undef,
      @_
   );

   # Retail Reductions(%) = Markdown(%) + Employee Discount(%) + Shrinkage(%)
   if (defined $args{markdown} && defined $args{employee_discount} &&
       defined $args{shrinkage})
   {
      return $args{markdown} + $args{employee_discount} + $args{shrinkage};
   }
   else
   {
      croak "No valid parameters passed.";
   }
}


sub cumulative_markon
{
   my %args = (
      gross_margin => undef,
      retail_reductions => undef,
      @_
   );

   # Cumulative Markon(%) = (Gross Margin(%) + Retail Reductions(%)) /
   #                        (100% + Retail Reductions(%))
   if (defined $args{gross_margin} && defined $args{retail_reductions})
   {
      return ($args{gross_margin} + $args{retail_reductions}) /
             (1 + $args{retail_reductions});
   }
   else
   {
      croak "No valid parameters passed.";
   }
}


sub gmroi
{
   my %args = (
      gross_margin => undef,
      average_inventory_cost => undef,
      @_
   );

   # GMROI = Gross Margin(%) / Average Inventory at Cost($)
   if (defined $args{gross_margin} && defined $args{average_inventory_cost})
   {
      return $args{gross_margin} / $args{average_inventory_cost};
   }
   else
   {
      croak "No valid parameters passed.";
   }
}


sub stock_shortage
{
   my %args = (
      stock_shortage => undef,
      net_sales => undef,
      @_
   );

   # Stock Shortage(%) = Stock Shortage($) / Net Sales($)
   if (defined $args{stock_shortage} && defined $args{net_sales})
   {
      return $args{stock_shortage} / $args{net_sales};
   }
   else
   {
      croak "No valid parameters passed.";
   }
}


1;


=pod

=head1 NAME

MCCS::RetailMath - Hodgepodge of retail math functions

=head2 DEFINITIONS/FORMULAS

=over 4

=item Retail

The amount a consumer pays for a given item. 
   Retail($) = Cost($) + Markup($)
   Retail($) = Cost($) / Cost Complement(%)
   Retail($) = Markup($) / Markup(%)

=item Cost

The amount a buyer pays for a given item from a vendor.
   Cost($) = Retail($) - Markup($)
   Cost($) = Retail($) * Cost Complement(%)

=item Markup (Markon)

The amount in addition to Cost that a given item Retails for.
   Markup($) = Retail($) - Cost($)
   Markup($) = Retail($) * Markup(%)
   Markup(%) = (Retail($) - Cost($)) / Retail($)
   Markup(%) = Markup($) / Retail($)
   Markup(%) = 100% - Cost Complement(%)

=item Cost Complement

The complement of the Markup percentage.
   Cost Complement(%) = 100% - Markup(%)
   Cost Complement(%) = Cost($) / Retail($)

=item Markdown

The amount from Retail that a given item Retails for.
   Markdown(%) = (Retail($) - New price($)) / New price($)
   Markdown(%) = Net Markdown($) / Net Sales($)

=item Savings

The amount from Retail that a given item Retails for during a promotion.
   Savings(%) = (Retail($) - New price($)) / Retail($)

=item Net Markups

The effective markups.
   Net Markups($) = Gross Markups($) - Markup Cancellations($)

=item Net Markdowns

The effective markdowns.
   Net Markdowns($) = Gross Markdowns($) - Markdown Cancellations($)

=item Turnover

The ratio that shows inventory utilization.
   Turnover = Sales($) / Average Inventory at Retail($)

=item Gross Margin

The profit earned before deductions of operating expenses.
   Gross Margin(%) = Gross Margin($) / Net Sales(%)
   Gross Margin(%) = Cumulative Markon(%) - (Cost Complement(%) * Retail Reductions(%))
   Gross Margin($) = Net Sales($) * Gross Margin(%)
   Gross Margin($) = Net Sales($) - Cost of Goods Sold($)

=item Retail Reductions %

The reductions incurred on retail sales.
   Retail Reductions(%) = Markdown(%) + Employee Discount(%) + Shrinkage(%)

=item Cumulative Markon

The markon across to all merchandise.
   Cumulative Markon(%) = (Gross Margin(%) + Retail Reductions(%)) / (100% + Retail Reductions(%))

=item GMROI (Gross Margin Return On Investment) [pron. Gim-roy]

The ratio that measures the efficiency of the investment of inventory.
   GMROI = Gross Margin(%) / Average Inventory at Cost($)

=item Stock Shortage

The deficiency in stock.
   Stock Shortage(%) = Stock Shortage($) / Net Sales($)

=back

=head2 FUNCTIONS

Unless otherwise specified all functions expect percentages as decimal values,
i.e. 34% as 0.34.

=over 4

=item retail(%params)

Calculate retail, given a set of one of the following named parameters:
   cost, markup
      for Retail($) = Cost($) + Markup($)
   cost, cost_complement
      for Retail($) = Cost($) / Cost Complement(%)
   markup_dols, markup_pct
      for Retail($) = Markup($) / Markup(%)

=item cost(%params)

Calculate cost, given a set of one of the following named parameters:
   retail, markup
      for Cost($) = Retail($) - Markup($)
   retail, cost_complement
      for Cost($) = Retail($) * Cost Complement(%)

=item markup_dols(%params)

Calculate markup dollars, given a set of one of the following named parameters:
   retail, cost
      for Markup($) = Retail($) - Cost($)
   retail, markup
      for Markup($) = Retail($) * Markup(%)

=item markup_pct(%params)

Calculate markup percentage, given a set of one of the following named
paramters:
   retail, cost
      for Markup(%) = (Retail($) - Cost($)) / Retail($)
   markup, retail
      for Markup(%) = Markup($) / Retail($)
   cost_complement
      for Markup(%) = 100% - Cost Complement(%)

=item cost_complement(%params)

Calculate cost complement, given a set of one of the following named paramters:
   markup
      for Cost Complement(%) = 100% - Markup(%)
   cost, retail
      for Cost Complement(%) = Cost($) / Retail($)

=item markdown(%params)

Calculate markdown, given a set of one of the following named paramters:
   retail, new_price
      for Markdown(%) = (Retail($) - New price($)) / New price($)
   net_markdown, net_sales
      for Markdown(%) = Net Markdown($) / Net Sales($)

=item savings(%params)

Calculate savings, given a set of one of the following named paramters:
   retail, new_price
      for Savings(%) = (Retail($) - New price($)) / Retail($)

=item net_markups(%params)

Calculate net markups, given a set of one of the following named paramters:
   gross_markups, markups_cancelled
      for Net Markups($) = Gross Markups($) - Markup Cancellations($)

=item net_markdowns(%params)

Calculate net markdowns, given a set of one of the following named paramters:
   gross_markdowns, markdowns_cancelled
      for Net Markdowns($) = Gross Markdowns($) - Markdown Cancellations($)

=item turnover(%params)

Calculate turnover, given a set of one of the following named paramters:
   sales, average_inventory_retail
      for Turnover = Sales($) / Average Inventory at Retail($)

=item gross_margin_pct(%params)

Calculate gross margin percentage, given a set of one of the following named
paramters:
   gross_margin, net_sales
      for Gross Margin(%) = Gross Margin($) / Net Sales(%)
   cumulative_markon, cost_complement, retail_reductions
      for Gross Margin(%) = Cumulative Markon(%) - (Cost Complement(%) * Retail Reductions(%))

=item gross_margin_dols(%params)

Calculate gross margin dollars, given a set of one of the following named
paramters:
   net_sales, gross_margin
      for Gross Margin($) = Net Sales($) * Gross Margin(%)
   net_sales, sold_goods_cost
      for Gross Margin($) = Net Sales($) - Cost of Goods Sold($)

=item retail_reductions(%params)

Calculate retail reductions, given a set of one of the following named
paramters:
   markdown, employee_discount, shrinkage
      for Retail Reductions(%) = Markdown(%) + Employee Discount(%) + Shrinkage(%)

=item cumulative_markon(%params)

Calculate cumulative markon, given a set of one of the following named
paramters:
   gross_margin, retail_reductions
      for Cumulative Markon(%) = (Gross Margin(%) + Retail Reductions(%)) / (100% + Retail Reductions(%))

=item gmroi(%params)

Calculate GMROI, given a set of one of the following named paramters:
   gross_margin, average_inventory_cost
      for GMROI = Gross Margin(%) / Average Inventory at Cost($)

=item stock_shortage(%params)

Calculate stock shortage, given a set of one of the following named paramters:
   stock_shortage, net_sales
      for Stock Shortage(%) = Stock Shortage($) / Net Sales($)

=back

=head1 BUGS

None so far.

=head1 HISTORY

=over 4

=item '2005-12-29'

Changed namespace to MCCS::Math::Retail.

=back

=head1 AUTHOR(S)

Joey Makar <makarjo@usmc-mccs.org>

=cut
