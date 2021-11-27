# SAMPLE REQUEST
# https://www.change.org/api-proxy/-/comments?limit=1000&offset=0&commentable_type=Event&commentable_id=20861011

import plotly.express as px

import numpy
import pandas

import utils
from change import petitions, tags, comments
from utils.google_services import save_list_to_sheets_tab

if __name__ == '__main__':
    all_pets = petitions.get()

    unmask_analyzed = comments.analyze_comments_from_petition_slugs(tags.nomask_slugs,
                                                                    topic_name='unmask',
                                                                    petitions_limit=100)
    promask_analyzed = comments.analyze_comments_from_petition_slugs(
        tags.promask_slugs,
        topic_name='promask', petitions_limit=100)

    unmask_analyzed_total = unmask_analyzed['count'].sum()
    promask_analyzed_total = promask_analyzed['count'].sum()

    unmask_analyzed = unmask_analyzed.head(200)
    promask_analyzed = promask_analyzed.head(200)

    unmask_analyzed = unmask_analyzed.assign(
        percentage=lambda x: x['count'] * 100 / unmask_analyzed_total,
        normalized=lambda x: numpy.log10(x['percentage']) / numpy.log10(4)
    )

    promask_analyzed = promask_analyzed.assign(
        percentage=lambda x: x['count'] * 100 / promask_analyzed_total,
        normalized=lambda x: numpy.log10(x['percentage']) / numpy.log10(4)
    )

    unmask_analyzed['normalized'] = unmask_analyzed['normalized'].apply(lambda x: x + 2)
    promask_analyzed['normalized'] = promask_analyzed['normalized'].apply(lambda x: x + 2)

    data = []

    merged = promask_analyzed.merge(unmask_analyzed, left_on='name', right_on='name', suffixes=('_promask', '_nomask'),
                                    how='outer')

    scatter = merged.loc[~merged['normalized_promask'].isna()].loc[~merged['normalized_nomask'].isna()]
    merged.fillna(0, inplace=True)

    merged = merged.assign(percentage_both=lambda x: x['percentage_nomask'] + x['percentage_promask'])
    merged.drop(columns=['source_promask', 'source_nomask'], inplace=True)
    merged.sort_values(by=['percentage_both'], ascending=False, inplace=True)

    utils.google_services.save_list_to_sheets_tab(merged, 'scatternonsopiu')

    scatter = scatter.assign(summed_percentage=lambda x: x['percentage_nomask'] + x['percentage_promask'])
    scatter.sort_values(by='summed_percentage', inplace=True, ascending=False)
    scatter.to_csv(utils.get_onedrive_path('csv', 'scatter-boh.csv'), index=False)

    scartate = pandas.concat([
        unmask_analyzed.loc[~unmask_analyzed['name'].isin(scatter['name'])],
        promask_analyzed.loc[~promask_analyzed['name'].isin(scatter['name'])],
    ], ignore_index=True)

    scartate.to_csv(utils.get_onedrive_path('csv', 'scatter-scartate.csv'), index=False)

    pandas.concat([promask_analyzed.assign(source='promask'), unmask_analyzed.assign(source='unmask')]).to_csv(
        utils.get_onedrive_path('csv', 'entietes_both.csv'))

    fig = px.scatter(scatter, x='normalized_nomask', y='normalized_promask', text='name', width=1500, height=1000)
    fig.update_traces(textposition='middle right', textfont=dict(
        size=16,
    ), marker=dict(
        size=10,
    ))

    # fig.update_xaxes(range=[-1.2, 1])
    # fig.update_yaxes(range=[-1.2, 1])

    fig.write_image(utils.get_onedrive_path('protocol3-scatter.svg'))
